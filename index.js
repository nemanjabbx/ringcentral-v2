const http = require('http');
const https = require('https');

const RC_CLIENT_ID = process.env.RC_CLIENT_ID;
const RC_CLIENT_SECRET = process.env.RC_CLIENT_SECRET;
const RC_JWT = process.env.RC_JWT;

let tokenCache = null;
let tokenExpiry = 0;

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
        try { resolve(JSON.parse(data)); }
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

async function checkAvailability(stateUpper) {
  const stateName = STATE_NAME_MAP[stateUpper] || stateUpper;
  const token = await getAccessToken();
  const queuesData = await getQueues(token);
  const queues = queuesData.records || [];

  const matchedQueue = queues.find(q =>
    q.name.toLowerCase() === stateName.toLowerCase()
  );

  if (!matchedQueue) {
    return {
      available: false,
      agents: 0,
      state: stateUpper,
      state_name: stateName,
      reason: `No queue found for: ${stateName}`
    };
  }

  const membersData = await getQueueMembers(token, matchedQueue.id);
  const members = membersData.records || [];

  const presenceResults = await Promise.all(
    members.map(m => getPresence(token, m.id).catch(() => null))
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
          const presence = await getPresence(token, extId);
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
  const queuesData = await getQueues(token);
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

  const membersData = await getQueueMembers(token, matchedQueue.id);
  const members = membersData.records || [];

  const presenceResults = await Promise.all(
    members.map(m => getPresence(token, m.id).catch(() => null))
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
    queue: matchedQueue.name,
    total_members: members.length
  };
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
    try {
      const result = await checkAvailability(state.toUpperCase().trim());
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

  // List all queues
  if (pathname === '/queues') {
    try {
      const token = await getAccessToken();
      const queuesData = await getQueues(token);
      const queues = (queuesData.records || []).map(q => ({ id: q.id, name: q.name }));
      res.writeHead(200);
      return res.end(JSON.stringify({ total: queues.length, queues }));
    } catch (err) {
      console.error('Error:', err.message);
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found. Use /availability?state=TX or /queue?name=QueueName' }));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Availability API running on port ${PORT}`));
