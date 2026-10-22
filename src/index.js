// aria-server Worker
// Gestisce autenticazione, WebSocket per voce in tempo reale e API per memoria e conversazioni

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Content-Type': 'application/json',
    },
  });
}

async function verifySupabaseToken(token, env) {
  const response = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
    headers: {
      'Authorization': `Bearer ${token}`,
      'apikey': env.SUPABASE_ANON_KEY,
    },
  });
  if (!response.ok) return null;
  return await response.json();
}

async function supabaseQuery(table, method, token, env, body = null, filters = '') {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}${filters}`;
  const options = {
    method,
    headers: {
      'Authorization': `Bearer ${token}`,
      'apikey': env.SUPABASE_ANON_KEY,
      'Content-Type': 'application/json',
    },
  };
  if (body) options.body = JSON.stringify(body);
  const response = await fetch(url, options);
  if (!response.ok) throw new Error(`Supabase error: ${response.status}`);
  const text = await response.text();
  return text ? JSON.parse(text) : null;
}

async function handleApi(request, env) {
  const auth = request.headers.get('Authorization');
  if (!auth?.startsWith('Bearer ')) return jsonResponse({ error: 'Token mancante' }, 401);
  const token = auth.slice(7);
  const user = await verifySupabaseToken(token, env);
  if (!user) return jsonResponse({ error: 'Token non valido' }, 401);

  const url = new URL(request.url);
  const path = url.pathname;

  if (path === '/api/memories' && request.method === 'GET') {
    const data = await supabaseQuery('memories', 'GET', token, env, null, `?user_id=eq.${user.id}&order=updated_at.desc`);
    return jsonResponse({ memories: data || [] });
  }

  if (path === '/api/memories' && request.method === 'POST') {
    const body = await request.json();
    const data = await supabaseQuery('memories', 'POST', token, env, {
      user_id: user.id,
      title: body.title,
      content: body.content,
    });
    return jsonResponse({ memory: data[0] }, 201);
  }

  if (path.startsWith('/api/memories/') && request.method === 'PUT') {
    const id = path.split('/').pop();
    const body = await request.json();
    const data = await supabaseQuery('memories', 'PATCH', token, env, {
      title: body.title,
      content: body.content,
      updated_at: new Date().toISOString(),
    }, `?id=eq.${id}&user_id=eq.${user.id}`);
    return jsonResponse({ memory: data[0] });
  }

  if (path.startsWith('/api/memories/') && request.method === 'DELETE') {
    const id = path.split('/').pop();
    await supabaseQuery('memories', 'DELETE', token, env, null, `?id=eq.${id}&user_id=eq.${user.id}`);
    return jsonResponse({ success: true });
  }

  if (path === '/api/memories' && request.method === 'DELETE') {
    await supabaseQuery('memories', 'DELETE', token, env, null, `?user_id=eq.${user.id}`);
    return jsonResponse({ success: true });
  }

  if (path === '/api/conversations' && request.method === 'POST') {
    const body = await request.json();
    await supabaseQuery('conversations', 'POST', token, env, {
      user_id: user.id,
      role: body.role,
      content: body.content,
    });
    return jsonResponse({ success: true }, 201);
  }

  return jsonResponse({ error: 'Non trovato' }, 404);
}

async function handleWebSocket(request, env) {
  const url = new URL(request.url);
  const token = url.searchParams.get('token');
  if (!token) return new Response('Token mancante', { status: 401 });
  const user = await verifySupabaseToken(token, env);
  if (!user) return new Response('Token non valido', { status: 401 });

  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);
  server.accept();

  let geminiWs = null;

  server.addEventListener('message', async (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === 'start' && !geminiWs) {
        const tokenRes = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/interactions?key=${env.GEMINI_API_KEY}`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              model: 'gemini-2.0-flash-exp',
              input: 'init',
              generation_config: { response_modalities: ['AUDIO'] },
            }),
          }
        );
        if (!tokenRes.ok) {
          server.send(JSON.stringify({ type: 'error', error: 'Token Gemini non disponibile' }));
          return;
        }
        const tokenData = await tokenRes.json();
        const ephemeralToken = tokenData.token || tokenData.ephemeral_token;

        const geminiUrl = `wss://generativelanguage.googleapis.com/v1beta/interactions?key=${env.GEMINI_API_KEY}`;
        geminiWs = new WebSocket(geminiUrl);
        geminiWs.addEventListener('open', () => server.send(JSON.stringify({ type: 'ready' })));
        geminiWs.addEventListener('message', (e) => {
          if (server.readyState === 1) server.send(e.data);
        });
        geminiWs.addEventListener('error', () => server.send(JSON.stringify({ type: 'error', error: 'Connessione Gemini fallita' })));
        geminiWs.addEventListener('close', () => server.send(JSON.stringify({ type: 'closed' })));
      } else if (msg.type === 'audio' && geminiWs?.readyState === 1) {
        geminiWs.send(JSON.stringify({ type: 'audio', data: msg.data }));
      } else if (msg.type === 'text' && geminiWs?.readyState === 1) {
        geminiWs.send(JSON.stringify({ type: 'text', text: msg.text }));
      }
    } catch (err) {
      server.send(JSON.stringify({ type: 'error', error: err.message }));
    }
  });

  server.addEventListener('close', () => { if (geminiWs?.readyState === 1) geminiWs.close(); });
  server.addEventListener('error', () => { if (geminiWs?.readyState === 1) geminiWs.close(); });

  return new Response(null, { status: 101, webSocket: client });
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        },
      });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    if (path.startsWith('/api/')) return handleApi(request, env);
    if (path === '/ws') return handleWebSocket(request, env);
    if (path === '/' || path === '/health') return jsonResponse({ status: 'ok' });

    return jsonResponse({ error: 'Non trovato' }, 404);
  },
}; 
