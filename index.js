const http = require('http');
const https = require('https');
const crypto = require('crypto');

const WEBHOOK_URL = process.env.WEBHOOK_URL || null;
const WEBHOOK_VERIFICATION_TOKEN = process.env.WEBHOOK_VERIFICATION_TOKEN || 'rc-webhook-verify-token';
let webhookSubscriptionId = null;
let webhookRenewalTimer = null;

const RC_CLIENT_ID = process.env.RC_CLIENT_ID;
const RC_CLIENT_SECRET = process.env.RC_CLIENT_SECRET;
const RC_JWT = process.env.RC_JWT;

let tokenCache = null;
let tokenExpiry = 0;

// --- RC API Rate Limiter (max 5 concurrent requests) ---
let rcActiveRequests = 0;
const RC_MAX_CONCURRENT = 5;
const rcQueue = [];

function rcThrottle(fn) {
  return new Promise((resolve, reject) => {
    const run = () => {
      rcActiveRequests++;
      fn().then(result => {
        resolve(result);
      }).catch(err => {
        reject(err);
      }).finally(() => {
        rcActiveRequests--;
        if (rcQueue.length > 0) rcQueue.shift()();
      });
    };
    if (rcActiveRequests < RC_MAX_CONCURRENT) {
      run();
    } else {
      rcQueue.push(run);
    }
  });
}

const presenceCache = new Map();
const PRESENCE_TTL = 15 * 1000; // 15 seconds (webhook updates instantly, this is fallback)

const queueMembersCache = new Map();
const QUEUE_MEMBERS_TTL = 30 * 60 * 1000; // 30 minutes (members rarely change)

let queuesCache = null;
let queuesCacheExpiry = 0;
const QUEUES_TTL = 5 * 60 * 1000; // 5 minutes

async function getPresenceCached(token, extensionId) {
  const now = Date.now();
  const cached = presenceCache.get(String(extensionId));
  if (cached && now < cached.expiry) return cached.data;
  try {
    const data = await rcThrottle(() => getPresence(token, extensionId));
    if (data) presenceCache.set(String(extensionId), { data, expiry: now + PRESENCE_TTL });
    return data;
  } catch (err) {
    if (cached) return cached.data;
    throw err;
  }
}

async function getQueueMembersCached(token, queueId) {
  const now = Date.now();
  const cached = queueMembersCache.get(queueId);
  if (cached && now < cached.expiry) return cached.data;
  try {
    const data = await getQueueMembers(token, queueId);
    queueMembersCache.set(queueId, { data, expiry: now + QUEUE_MEMBERS_TTL });
    return data;
  } catch (err) {
    if (cached) return cached.data; // return stale cache on rate limit
    throw err;
  }
}

async function getQueuesCached(token) {
  const now = Date.now();
  if (queuesCache && now < queuesCacheExpiry) return queuesCache;
  try {
    const data = await getQueues(token);
    queuesCache = data;
    queuesCacheExpiry = now + QUEUES_TTL;
    return data;
  } catch (err) {
    if (queuesCache) return queuesCache; // return stale cache on rate limit
    throw err;
  }
}

const STATE_NAME_MAP = {
  'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
  'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
  'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
  'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
  'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
  'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
  'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
  'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
  'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
  'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
  'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
  'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'DC': 'Washington, DC',
  'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
};

async function getAccessToken() {
  const now = Date.now();
  if (tokenCache && now < tokenExpiry) {
    return tokenCache;
  }
  return new Promise((resolve, reject) => {
    const credentials = Buffer.from(`${RC_CLIENT_ID}:${RC_CLIENT_SECRET}`).toString('base64');
    const body = `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${RC_JWT}`;
    const options = {
      hostname: 'platform.ringcentral.com',
      path: '/restapi/oauth/token',
      method: 'POST',
      headers: {
        'Authorization': `Basic ${credentials}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.access_token) {
            tokenCache = json.access_token;
            tokenExpiry = now + (55 * 60 * 1000);
            resolve(tokenCache);
          } else {
            reject(new Error('No access token: ' + data));
          }
        } catch(e) {
          reject(new Error('Failed to parse token response: ' + data));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function getQueues(token) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: '/restapi/v1.0/account/~/call-queues?perPage=200',
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(new Error('Failed to parse queues: ' + data)); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function getExtensions(token) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: '/restapi/v1.0/account/~/extension?perPage=200&type=User&status=Enabled',
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(new Error('Failed to parse extensions: ' + data)); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

let extensionsCache = null;
let extensionsCacheExpiry = 0;
const EXTENSIONS_TTL = 10 * 60 * 1000; // 10 minutes

async function getExtensionsCached(token) {
  const now = Date.now();
  if (extensionsCache && now < extensionsCacheExpiry) return extensionsCache;
  try {
    const data = await getExtensions(token);
    extensionsCache = data;
    extensionsCacheExpiry = now + EXTENSIONS_TTL;
    return data;
  } catch (err) {
    if (extensionsCache) return extensionsCache;
    throw err;
  }
}

async function getQueueMembers(token, queueId) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: `/restapi/v1.0/account/~/call-queues/${queueId}/members?perPage=200`,
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.errorCode) reject(new Error(`RC error ${json.errorCode}: ${json.message}`));
          else resolve(json);
        }
        catch(e) { reject(new Error('Failed to parse members: ' + data)); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function getPresence(token, extensionId) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: `/restapi/v1.0/account/~/extension/${extensionId}/presence`,
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.end();
  });
}

async function checkAvailability(stateUpper, office) {
  const stateName = STATE_NAME_MAP[stateUpper] || stateUpper;
  const queueName = office ? `${stateName} - ${office}` : stateName;
  const token = await getAccessToken();
  const queuesData = await getQueuesCached(token);
  const queues = queuesData.records || [];

  const matchedQueue = queues.find(q =>
    q.name.toLowerCase() === queueName.toLowerCase()
  );

  if (!matchedQueue) {
    return {
      available: false,
      agents: 0,
      state: stateUpper,
      state_name: stateName,
      office: office || 'main',
      reason: `No queue found for: ${queueName}`
    };
  }

  const membersData = await getQueueMembersCached(token, matchedQueue.id);
  const members = membersData.records || [];

  const presenceResults = await Promise.all(
    members.map(m => getPresenceCached(token, m.id).catch(() => null))
  );

  const availableAgents = presenceResults.filter(p => {
    if (!p) return false;
    return (
      p.presenceStatus === 'Available' &&
      p.dndStatus === 'TakeAllCalls' &&
      p.telephonyStatus === 'NoCall'
    );
  });

  return {
    available: availableAgents.length > 0,
    agents: availableAgents.length,
    state: stateUpper,
    state_name: stateName,
    office: office || 'main',
    queue: matchedQueue.name,
    total_members: members.length
  };
}

async function checkAgentByExtension(ext) {
  const token = await getAccessToken();
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: `/restapi/v1.0/account/~/extension?extensionNumber=${ext}`,
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', async () => {
        try {
          const json = JSON.parse(data);
          const records = json.records || [];
          if (records.length === 0) {
            return resolve({ available: false, error: `No extension found: ${ext}` });
          }
          const extId = records[0].id;
          const extName = records[0].name;
          const presence = await getPresenceCached(token, extId);
          if (!presence) return resolve({ available: false, extension: ext, name: extName });
          const available = (
            presence.presenceStatus === 'Available' &&
            presence.dndStatus === 'TakeAllCalls' &&
            presence.telephonyStatus === 'NoCall'
          );
          resolve({
            available,
            extension: ext,
            name: extName,
            presenceStatus: presence.presenceStatus,
            dndStatus: presence.dndStatus,
            telephonyStatus: presence.telephonyStatus
          });
        } catch(e) {
          reject(new Error('Failed to parse extension response: ' + data));
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function checkQueueAvailability(queueName) {
  const token = await getAccessToken();
  const queuesData = await getQueuesCached(token);
  const queues = queuesData.records || [];

  const matchedQueue = queues.find(q =>
    q.name.toLowerCase() === queueName.toLowerCase()
  );

  if (!matchedQueue) {
    return {
      available: false,
      agents: 0,
      reason: `No queue found for: ${queueName}`
    };
  }

  const membersData = await getQueueMembersCached(token, matchedQueue.id);
  const members = membersData.records || [];

  const presenceResults = await Promise.all(
    members.map(m => getPresenceCached(token, m.id).catch(() => null))
  );

  const availableAgents = presenceResults.filter(p => {
    if (!p) return false;
    return (
      p.presenceStatus === 'Available' &&
      p.dndStatus === 'TakeAllCalls' &&
      p.telephonyStatus === 'NoCall'
    );
  });

  const activeCalls = presenceResults.filter(p => {
    if (!p) return false;
    return p.telephonyStatus === 'CallConnected' || p.telephonyStatus === 'OnHold' || p.telephonyStatus === 'Ringing';
  }).length;

  return {
    available: availableAgents.length > 0,
    agents: availableAgents.length,
    active_calls: activeCalls,
    queue: matchedQueue.name,
    total_members: members.length
  };
}

async function checkAvailabilityWithMinAgents(stateUpper, office, minAgents) {
  const result = await checkAvailability(stateUpper, office);
  if (minAgents && result.agents < minAgents) {
    return { ...result, available: false, reason: `Not enough agents: ${result.agents} available, ${minAgents} required` };
  }
  return result;
}

const server = http.createServer(async (req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');

  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = url.pathname;

  if (pathname === '/' || pathname === '/health') {
    res.writeHead(200);
    return res.end(JSON.stringify({ status: 'ok', message: 'Availability API is running' }));
  }

  // State-based: /availability?state=TX
  if (pathname === '/availability') {
    const state = url.searchParams.get('state');
    if (!state) {
      res.writeHead(400);
      return res.end(JSON.stringify({ available: false, error: 'Missing state parameter. Use ?state=TX' }));
    }
    const office = url.searchParams.get('office') ? url.searchParams.get('office').trim() : null;
    const minAgentsParam = url.searchParams.get('min_agents');
    const minAgents = minAgentsParam ? parseInt(minAgentsParam, 10) : null;
    try {
      const result = await checkAvailabilityWithMinAgents(state.toUpperCase().trim(), office, minAgents);
      res.writeHead(200);
      return res.end(JSON.stringify(result));
    } catch (err) {
      console.error('Error:', err.message);
      res.writeHead(500);
      return res.end(JSON.stringify({ available: false, error: err.message }));
    }
  }

  // Queue-based: /queue?name=Sales+Team
  if (pathname === '/queue') {
    const name = url.searchParams.get('name');
    if (!name) {
      res.writeHead(400);
      return res.end(JSON.stringify({ available: false, error: 'Missing name parameter. Use ?name=QueueName' }));
    }
    try {
      const result = await checkQueueAvailability(name.trim());
      res.writeHead(200);
      return res.end(JSON.stringify(result));
    } catch (err) {
      console.error('Error:', err.message);
      res.writeHead(500);
      return res.end(JSON.stringify({ available: false, error: err.message }));
    }
  }

  // Debug: raw presence status for all agents
  if (pathname === '/agents/debug') {
    try {
      const token = await getAccessToken();
      const extData = await getExtensionsCached(token);
      const extensions = (extData.records || []).map(e => e.extensionNumber);
      const results = await Promise.all(extensions.map(async (ext) => {
        try {
          const json = await new Promise((resolve, reject) => {
            const options = {
              hostname: 'platform.ringcentral.com',
              path: `/restapi/v1.0/account/~/extension?extensionNumber=${ext}`,
              method: 'GET',
              headers: { 'Authorization': `Bearer ${token}` }
            };
            const req = https.request(options, (res) => {
              let data = '';
              res.on('data', chunk => data += chunk);
              res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { resolve(null); } });
            });
            req.on('error', reject);
            req.end();
          });
          const records = json && json.records || [];
          if (!records.length) return { extension: ext, error: 'not found' };
          const extId = records[0].id;
          const extName = records[0].name;
          const presence = await getPresenceCached(token, extId);
          return {
            extension: ext,
            name: extName,
            presenceStatus: presence ? presence.presenceStatus : null,
            dndStatus: presence ? presence.dndStatus : null,
            telephonyStatus: presence ? presence.telephonyStatus : null,
            userStatus: presence ? presence.userStatus : null,
            raw: presence
          };
        } catch(e) {
          return { extension: ext, error: e.message };
        }
      }));
      res.writeHead(200);
      return res.end(JSON.stringify({ agents: results }));
    } catch (err) {
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  // All agents status: /agents
  if (pathname === '/agents') {
    try {
      const token = await getAccessToken();
      const extsParam = url.searchParams.get('exts');
      let extensions;
      if (extsParam) {
        extensions = extsParam.split(',').map(e => e.trim());
      } else {
        const extData = await getExtensionsCached(token);
        extensions = (extData.records || []).map(e => e.extensionNumber);
      }
      const results = await Promise.all(extensions.map(e => checkAgentByExtension(e).catch(err => ({ available: false, extension: e, error: err.message }))));
      const available = results.filter(r => r.available);
      res.writeHead(200);
      return res.end(JSON.stringify({
        available: available.length > 0,
        available_count: available.length,
        total: results.length,
        agents: results
      }));
    } catch (err) {
      console.error('Error:', err.message);
      res.writeHead(500);
      return res.end(JSON.stringify({ available: false, error: err.message }));
    }
  }

  // Only available agents: /agents/available
  if (pathname === '/agents/available') {
    try {
      const token = await getAccessToken();
      const extData = await getExtensionsCached(token);
      const extensions = (extData.records || []).map(e => e.extensionNumber);
      const results = await Promise.all(extensions.map(e => checkAgentByExtension(e).catch(() => ({ available: false, extension: e }))));
      const available = results.filter(r => r.available);
      res.writeHead(200);
      return res.end(JSON.stringify({
        available: available.length > 0,
        available_count: available.length,
        agents: available
      }));
    } catch (err) {
      console.error('Error:', err.message);
      res.writeHead(500);
      return res.end(JSON.stringify({ available: false, error: err.message }));
    }
  }

  // Agent check by extension: /agent?ext=106
  if (pathname === '/agent') {
    const ext = url.searchParams.get('ext');
    if (!ext) {
      res.writeHead(400);
      return res.end(JSON.stringify({ available: false, error: 'Missing ext parameter. Use ?ext=106' }));
    }
    try {
      const result = await checkAgentByExtension(ext.trim());
      res.writeHead(200);
      return res.end(JSON.stringify(result));
    } catch (err) {
      console.error('Error:', err.message);
      res.writeHead(500);
      return res.end(JSON.stringify({ available: false, error: err.message }));
    }
  }

  // Queue shortcuts
  const QUEUE_SHORTCUTS = {
    '/queue/vip': 'VIP Response',
    '/queue/120': 'Lead by Call - 120s',
    '/queue/90': 'Lead by Call - 90s',
    '/queue/ringbax': 'RingbaX'
  };
  if (QUEUE_SHORTCUTS[pathname]) {
    try {
      const result = await checkQueueAvailability(QUEUE_SHORTCUTS[pathname]);
      res.writeHead(200);
      return res.end(JSON.stringify(result));
    } catch (err) {
      console.error('Error:', err.message);
      res.writeHead(500);
      return res.end(JSON.stringify({ available: false, error: err.message }));
    }
  }

  // Debug queue members + their presence
  if (pathname === '/queue/members/debug') {
    const name = url.searchParams.get('name') || 'VIP Response';
    try {
      const token = await getAccessToken();
      const queuesData = await getQueuesCached(token);
      const queues = queuesData.records || [];
      const matchedQueue = queues.find(q => q.name.toLowerCase() === name.toLowerCase());
      if (!matchedQueue) {
        res.writeHead(200);
        return res.end(JSON.stringify({ error: `Queue not found: ${name}`, available_queues: queues.map(q => q.name) }));
      }
      const membersData = await getQueueMembersCached(token, matchedQueue.id);
      const members = membersData.records || [];
      const withPresence = await Promise.all(members.map(async (m) => {
        const presence = await getPresenceCached(token, m.id).catch(() => null);
        return {
          id: m.id,
          name: m.name,
          extensionNumber: m.extensionNumber,
          presenceStatus: presence ? presence.presenceStatus : null,
          dndStatus: presence ? presence.dndStatus : null,
          telephonyStatus: presence ? presence.telephonyStatus : null,
          raw: presence
        };
      }));
      res.writeHead(200);
      return res.end(JSON.stringify({ queue: matchedQueue.name, total_members: members.length, members: withPresence }));
    } catch (err) {
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  // List all queues
  if (pathname === '/queues') {
    try {
      const token = await getAccessToken();
      const queuesData = await getQueuesCached(token);
      const queues = (queuesData.records || []).map(q => ({ id: q.id, name: q.name }));
      res.writeHead(200);
      return res.end(JSON.stringify({ total: queues.length, queues }));
    } catch (err) {
      console.error('Error:', err.message);
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  // RC Webhook receiver
  if (pathname === '/webhook/presence') {
    const validationToken = req.headers['validation-token'];
    if (validationToken) {
      console.log('Webhook: validation request received');
      res.writeHead(200, { 'Validation-Token': validationToken });
      return res.end();
    }
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      console.log('Webhook: incoming payload:', body.slice(0, 300));
      handleWebhookPresence(body);
      res.writeHead(200);
      res.end();
    });
    return;
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found. Use /availability?state=TX or /queue?name=QueueName' }));
});

async function warmupCache() {
  try {
    console.log('Warming up cache...');
    const token = await getAccessToken();
    const queuesData = await getQueuesCached(token);
    const queues = queuesData.records || [];

    const allMembersData = await Promise.all(
      queues.map(q => getQueueMembersCached(token, q.id).catch(() => null))
    );

    const uniqueExtIds = new Set();
    allMembersData.forEach(md => {
      if (md && md.records) md.records.forEach(m => uniqueExtIds.add(m.id));
    });

    // Fetch presence one by one with small delay to avoid rate limit during warmup
    for (const extId of uniqueExtIds) {
      await getPresenceCached(token, extId).catch(() => null);
      await new Promise(r => setTimeout(r, 200));
    }

    console.log(`Cache warmed: ${queues.length} queues, ${uniqueExtIds.size} agents`);
  } catch (err) {
    console.error('Cache warmup failed:', err.message);
  }
}

async function createWebhookSubscription(token) {
  if (!WEBHOOK_URL) {
    console.log('WEBHOOK_URL not set, skipping webhook subscription');
    return;
  }
  return new Promise((resolve) => {
    const body = JSON.stringify({
      eventFilters: [
        '/restapi/v1.0/account/~/extension/~/presence?detailedTelephonyState=true'
      ],
      deliveryMode: {
        transportType: 'WebHook',
        address: `${WEBHOOK_URL}/webhook/presence`,
        verificationToken: WEBHOOK_VERIFICATION_TOKEN
      },
      expiresIn: 86400
    });
    const options = {
      hostname: 'platform.ringcentral.com',
      path: '/restapi/v1.0/subscription',
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.id) {
            webhookSubscriptionId = json.id;
            console.log(`RC webhook subscription created: ${json.id}`);
            scheduleWebhookRenewal();
            resolve(json);
          } else {
            console.error('Webhook subscription failed:', data);
            resolve(null);
          }
        } catch(e) { resolve(null); }
      });
    });
    req.on('error', (err) => { console.error('Webhook subscription error:', err.message); resolve(null); });
    req.write(body);
    req.end();
  });
}

function scheduleWebhookRenewal() {
  if (webhookRenewalTimer) clearTimeout(webhookRenewalTimer);
  webhookRenewalTimer = setTimeout(async () => {
    try {
      console.log('Renewing RC webhook subscription...');
      const token = await getAccessToken();
      await createWebhookSubscription(token);
    } catch(err) {
      console.error('Webhook renewal failed:', err.message);
    }
  }, 23 * 60 * 60 * 1000);
}

function handleWebhookPresence(body) {
  try {
    const data = JSON.parse(body);
    const presence = data.body || data;
    const extensionId = presence.extensionId ||
      (presence.extension && presence.extension.id) ||
      (data.body && data.body.extension && data.body.extension.id);
    if (!extensionId) {
      console.log('Webhook: no extensionId found in payload:', JSON.stringify(data).slice(0, 200));
      return;
    }
    presenceCache.set(String(extensionId), {
      data: presence,
      expiry: Date.now() + (60 * 1000)
    });
    console.log(`Webhook: updated presence for ext ${extensionId} → ${presence.presenceStatus} / ${presence.telephonyStatus}`);
  } catch(e) {
    console.error('Webhook parse error:', e.message);
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, async () => {
  console.log(`Availability API running on port ${PORT}`);
  await warmupCache();
  try {
    const token = await getAccessToken();
    await createWebhookSubscription(token);
  } catch(err) {
    console.error('Failed to create webhook subscription:', err.message);
  }
});
