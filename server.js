const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const MAX_ACTIVE_PLAYERS = 16;

// Create HTTP server (required by free hosts like Render for health checks)
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Co-Play Relay Online\n');
});

const wss = new WebSocketServer({ server });

// Room state: tracks active entities (Host + up to 16 Players) and Ghosts
let hostId = null;
const activePlayers = new Map(); // id -> { id, role, x, z, color }
const ghosts = new Set();        // Set of WebSocket connections

function broadcast(payload, excludeWs = null) {
  const raw = JSON.stringify(payload);
  for (const client of wss.clients) {
    if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
      client.send(raw);
    }
  }
}

function getPopulationStats() {
  return {
    type: 'stats',
    activeCount: activePlayers.size,
    ghostCount: ghosts.size,
    hostId
  };
}

wss.on('connection', (ws) => {
  const id = crypto.randomBytes(4).toString('hex');
  ws.clientId = id;

  // Assign Role: 1st user = Host, next 16 = Active Players, rest = Ghosts
  let role = 'ghost';
  if (!hostId) {
    hostId = id;
    role = 'host';
  } else if (activePlayers.size < MAX_ACTIVE_PLAYERS) {
    role = 'player';
  }

  if (role === 'host' || role === 'player') {
    activePlayers.set(id, {
      id,
      role,
      x: (Math.random() - 0.5) * 14,
      z: (Math.random() - 0.5) * 14
    });
  } else {
    ghosts.add(ws);
  }

  // Send initial welcome state to the newly connected client
  ws.send(JSON.stringify({
    type: 'init',
    id,
    role,
    hostId,
    players: Array.from(activePlayers.values()),
    ghostCount: ghosts.size
  }));

  // Notify everyone else of new player or updated ghost count
  if (role !== 'ghost') {
    broadcast({ type: 'player_joined', player: activePlayers.get(id) }, ws);
  }
  broadcast(getPopulationStats());

  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch {
      return;
    }

    // 1. Movement updates (ONLY accepted from Host & Active Players)
    if (msg.type === 'move' && activePlayers.has(id)) {
      const p = activePlayers.get(id);
      p.x = Number(msg.x) || 0;
      p.z = Number(msg.z) || 0;
      broadcast({ type: 'move', id, x: p.x, z: p.z }, ws);
    }

    // 2. Crowd Triggers & Emotes (Accepted from ANYONE, including Ghosts)
    if (msg.type === 'emote') {
      broadcast({
        type: 'emote',
        id,
        emote: String(msg.emote).slice(0, 4)
      });
    }

    if (msg.type === 'trigger_meteor') {
      broadcast({
        type: 'meteor',
        x: (Math.random() - 0.5) * 24,
        z: (Math.random() - 0.5) * 24,
        sender: id
      });
    }

    // 3. Role Swap ("Jump In" Queue / "Become Ghost")
    if (msg.type === 'request_role') {
      if (msg.role === 'ghost' && activePlayers.has(id) && id !== hostId) {
        activePlayers.delete(id);
        ghosts.add(ws);
        ws.send(JSON.stringify({ type: 'role_changed', role: 'ghost' }));
        broadcast({ type: 'player_left', id });
        broadcast(getPopulationStats());
      } else if (msg.role === 'player' && ghosts.has(ws)) {
        if (activePlayers.size < MAX_ACTIVE_PLAYERS) {
          ghosts.delete(ws);
          const newPlayer = {
            id,
            role: 'player',
            x: (Math.random() - 0.5) * 14,
            z: (Math.random() - 0.5) * 14
          };
          activePlayers.set(id, newPlayer);
          ws.send(JSON.stringify({ type: 'role_changed', role: 'player', player: newPlayer }));
          broadcast({ type: 'player_joined', player: newPlayer }, ws);
          broadcast(getPopulationStats());
        } else {
          ws.send(JSON.stringify({ type: 'queue_full' }));
        }
      }
    }
  });

  ws.on('close', () => {
    ghosts.delete(ws);
    if (activePlayers.has(id)) {
      activePlayers.delete(id);
      broadcast({ type: 'player_left', id });
    }
    if (id === hostId) {
      // Promote next active player to Host if Host leaves
      const next = activePlayers.values().next().value;
      hostId = next ? next.id : null;
      if (next) {
        next.role = 'host';
        broadcast({ type: 'new_host', hostId });
      }
    }
    broadcast(getPopulationStats());
  });
});

server.listen(PORT, () => {
  console.log(`Co-Play Relay listening on port ${PORT}`);
});