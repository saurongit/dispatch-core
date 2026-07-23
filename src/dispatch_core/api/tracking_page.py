# ruff: noqa: E501
from __future__ import annotations


def render_tracking_page(nonce: str) -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
  <meta name="robots" content="noindex,nofollow,noarchive">
  <title>Статус заявки</title>
  <link rel="stylesheet"
        href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
        integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
        crossorigin="">
  <style nonce="{nonce}">
    :root {{ color-scheme: dark; font-family: Inter, system-ui, sans-serif; --bg: #071426; --panel: #0b1a2f; --blue: #00a3ff; --orange: #ff6b00; --green: #22c55e; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; min-width: 300px; background: radial-gradient(circle at 50% 0, #102a43 0, var(--bg) 44%); color: #f7fafc; }}
    main {{ min-height: 100dvh; padding: 18px 14px 12px; }}
    .shell {{ width: min(100%, 1080px); margin: 0 auto; }}
    header {{ padding: 16px 4px 18px; text-align: center; }}
    .eyebrow {{ margin: 0 0 8px; color: var(--blue); font-size: 11px; font-weight: 800; letter-spacing: .28em; text-transform: uppercase; }}
    h1 {{ margin: 0; font-size: clamp(28px, 7vw, 48px); line-height: 1.08; }}
    #summary, #address {{ margin: 9px auto 0; max-width: 760px; color: #a9b8c8; line-height: 1.45; }}
    #address {{ margin-top: 3px; font-size: 14px; }}
    #status-flow {{ display: flex; justify-content: center; align-items: center; margin: 0 auto 16px; padding: 9px 12px; width: fit-content; max-width: 100%; border: 1px solid #ffffff1a; border-radius: 999px; background: #ffffff08; }}
    .step {{ display: flex; align-items: center; }}
    .step-dot {{ display: grid; place-items: center; width: 34px; height: 34px; border-radius: 50%; background: #ffffff12; color: #8395a7; transition: .25s ease; }}
    .step.active .step-dot {{ background: var(--orange); color: var(--panel); box-shadow: 0 0 18px #ff6b0060; }}
    .step-line {{ width: clamp(12px, 6vw, 40px); height: 2px; background: #ffffff16; }}
    .step.active + .step-line, .step-line.active {{ background: var(--orange); }}
    .map-wrap {{ position: relative; min-height: 430px; height: min(62dvh, 620px); overflow: hidden; border: 1px solid #ffffff1a; border-radius: 24px; background: var(--panel); box-shadow: 0 20px 60px #0006; }}
    #map {{ width: 100%; height: 100%; display: none; }}
    #empty {{ position: absolute; inset: 0; display: grid; place-items: center; padding: 32px; text-align: center; color: #a9b8c8; }}
    #map-status {{ position: absolute; z-index: 1000; top: 12px; right: 12px; max-width: calc(100% - 24px); padding: 9px 12px; border: 1px solid #ffffff1a; border-radius: 12px; background: #0b1a2fe8; box-shadow: 0 8px 24px #0004; font-size: 13px; }}
    #updated {{ display: block; margin-top: 3px; color: #91a3b5; font-size: 10px; }}
    .legend {{ position: absolute; z-index: 1000; bottom: 24px; left: 12px; display: flex; gap: 14px; padding: 8px 11px; border-radius: 10px; background: #0b1a2fe0; font-size: 12px; }}
    .legend span {{ display: inline-flex; align-items: center; gap: 6px; }}
    .swatch {{ width: 11px; height: 11px; border-radius: 50%; }}
    footer {{ padding: 11px 4px 0; color: #91a3b5; font-size: 12px; text-align: center; }}
    footer a {{ color: #b9dfff; }}
    @keyframes siren-ring {{ 0% {{ transform: scale(.65); opacity: .9; }} 100% {{ transform: scale(1.55); opacity: 0; }} }}
    @keyframes siren-pulse {{ 0%,100% {{ transform: scale(.94); }} 50% {{ transform: scale(1.06); }} }}
    .client-pin, .master-pin {{ position: relative; width: 52px; height: 52px; }}
    .client-pin::before, .master-pin::before {{ content: ''; position: absolute; inset: 1px; border: 3px solid var(--orange); border-radius: 50%; animation: siren-ring 1.5s ease-out infinite; }}
    .client-pin b {{ position: absolute; inset: 10px; border-radius: 50%; background: var(--orange); box-shadow: 0 0 22px #ff6b0090; }}
    .client-pin i {{ position: absolute; inset: 17px; border-radius: 50%; background: white; }}
    .master-pin b {{ position: absolute; inset: 3px; display: grid; place-items: center; border-radius: 50%; background: var(--orange); box-shadow: 0 0 28px #ff6b0080; animation: siren-pulse 1.3s ease-in-out infinite; font-size: 22px; font-style: normal; }}
    .master-pin b::after {{ content: ''; position: absolute; inset: 9px; z-index: -1; border-radius: 50%; background: var(--panel); }}
    .master-pin.arrived::before {{ border-color: var(--green); }}
    .master-pin.arrived b {{ background: var(--green); box-shadow: 0 0 28px #22c55e70; animation: none; }}
    .leaflet-control-attribution {{ font-size: 10px; }}
    @media (max-width: 520px) {{ main {{ padding: 10px 8px 8px; }} .map-wrap {{ min-height: 400px; height: 61dvh; border-radius: 18px; }} .step-dot {{ width: 30px; height: 30px; font-size: 13px; }} .legend {{ bottom: 26px; }} }}
  </style>
</head>
<body>
<main>
  <div class="shell">
    <header>
      <p class="eyebrow">Live tracking</p>
      <h1 id="title">Статус заявки</h1>
      <p id="summary">Подключаемся к трекингу…</p>
      <p id="address"></p>
    </header>
    <div id="status-flow" aria-label="Этапы выполнения заявки">
      <div class="step" data-step="0"><span class="step-dot" title="Заявка принята">🆕</span></div><span class="step-line"></span>
      <div class="step" data-step="1"><span class="step-dot" title="Мастер назначен">👤</span></div><span class="step-line"></span>
      <div class="step" data-step="2"><span class="step-dot" title="Мастер в пути">🚗</span></div><span class="step-line"></span>
      <div class="step" data-step="3"><span class="step-dot" title="Мастер на месте">📍</span></div>
    </div>
    <div class="map-wrap">
      <div id="empty">Карта появится, когда будет известна точка объекта или мастер отправит геопозицию.</div>
      <div id="map" aria-label="Место выполнения работы и положение мастера"></div>
      <div id="map-status"><span id="map-status-text">Ожидаем данные…</span><span id="updated"></span></div>
      <div class="legend"><span><i class="swatch" style="background:#ff6b00"></i>Место выполнения работы</span><span><i class="swatch" style="background:#00a3ff"></i>Мастер</span></div>
    </div>
    <footer>Карта: © <a href="https://www.openstreetmap.org/copyright" rel="noopener">OpenStreetMap contributors</a></footer>
  </div>
</main>
<script nonce="{nonce}"
        src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
        integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
        crossorigin=""></script>
<script nonce="{nonce}">
(() => {{
  'use strict';
  const token = decodeURIComponent(location.hash.slice(1));
  const summary = document.getElementById('summary');
  const address = document.getElementById('address');
  const title = document.getElementById('title');
  const mapNode = document.getElementById('map');
  const empty = document.getElementById('empty');
  const mapStatus = document.getElementById('map-status-text');
  const updated = document.getElementById('updated');
  let map = null;
  let clientMarker = null;
  let masterMarker = null;
  let timer = null;
  let loadedOnce = false;

  const labels = {{
    submitted: 'Заявка принята',
    pool_open: 'Ищем подходящего мастера',
    assigned: 'Мастер назначен',
    accepted: 'Мастер принял заявку',
    en_route: 'Мастер в пути',
    in_progress: 'Мастер на месте',
    completed: 'Работа завершена',
    cancelled: 'Заявка отменена'
  }};
  const icons = {{ submitted: '🆕', pool_open: '🔎', assigned: '👤', accepted: '👤', en_route: '🚗', in_progress: '📍', completed: '✅', cancelled: '❌' }};
  const stages = {{ submitted: 0, pool_open: 0, assigned: 1, accepted: 1, en_route: 2, in_progress: 3, completed: 3 }};

  function stop(message) {{
    if (timer !== null) clearTimeout(timer);
    summary.textContent = message;
  }}

  function pointIcon(kind, arrived) {{
    const html = kind === 'client'
      ? '<div class="client-pin"><b></b><i></i></div>'
      : '<div class="master-pin' + (arrived ? ' arrived' : '') + '"><b>🚗</b></div>';
    return L.divIcon({{ className: '', html, iconSize: [52, 52], iconAnchor: [26, 26] }});
  }}

  function ensureMap(center) {{
    empty.style.display = 'none';
    mapNode.style.display = 'block';
    if (map === null) {{
      map = L.map(mapNode, {{ zoomControl: true }}).setView(center, 14);
      L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
        maxZoom: 19,
        attribution: '© OpenStreetMap contributors'
      }}).addTo(map);
    }}
  }}

  function showPoints(data) {{
    const client = data.client_point;
    const master = data.latest_point;
    const center = client || master;
    if (!center) return;
    ensureMap([center.latitude, center.longitude]);
    const bounds = [];
    if (client) {{
      const pos = [client.latitude, client.longitude];
      bounds.push(pos);
      if (clientMarker === null) clientMarker = L.marker(pos, {{ icon: pointIcon('client', false), title: 'Место выполнения работы' }}).addTo(map);
      else clientMarker.setLatLng(pos);
    }}
    if (master) {{
      const pos = [master.latitude, master.longitude];
      bounds.push(pos);
      const icon = pointIcon('master', data.order_status === 'in_progress' || data.order_status === 'completed');
      if (masterMarker === null) masterMarker = L.marker(pos, {{ icon, title: 'Мастер' }}).addTo(map);
      else {{ masterMarker.setLatLng(pos); masterMarker.setIcon(icon); }}
    }}
    if (bounds.length > 1) map.fitBounds(bounds, {{ padding: [64, 64], maxZoom: 15 }});
    else map.setView(bounds[0], 15, {{ animate: true }});
  }}

  function showProgress(status) {{
    const current = stages[status];
    document.querySelectorAll('.step').forEach(node => node.classList.toggle('active', current !== undefined && Number(node.dataset.step) <= current));
    document.querySelectorAll('.step-line').forEach((node, index) => node.classList.toggle('active', current !== undefined && index < current));
  }}

  async function refresh() {{
    if (token.length < 43) {{
      stop('Ссылка трекинга недействительна.');
      return;
    }}
    try {{
      const response = await fetch('/v1/public/tracking', {{
        method: 'GET',
        cache: 'no-store',
        credentials: 'omit',
        headers: {{ 'X-Tracking-Token': token }}
      }});
      if (!response.ok) {{
        if (response.status === 404 && loadedOnce) {{
          title.textContent = 'Трекинг завершён';
          stop('Заявка закрыта или отменена. Передача координат прекращена.');
        }} else {{
          stop(response.status === 404 ? 'Ссылка трекинга недействительна или уже закрыта.' : 'Не удалось обновить данные.');
        }}
        return;
      }}
      const data = await response.json();
      loadedOnce = true;
      title.textContent = 'Заявка ' + data.public_number + ' — ' + (labels[data.order_status] || 'статус');
      const parts = [data.brand, data.work_type, data.master_name];
      address.textContent = data.address || '';
      showProgress(data.order_status);
      showPoints(data);
      mapStatus.textContent = (icons[data.order_status] || '🗺️') + ' ' + (labels[data.order_status] || 'Ожидаем данные');
      if (data.latest_point) {{
        updated.textContent = 'обновлено: ' + new Date(data.latest_point.captured_at).toLocaleTimeString('ru-RU', {{ hour: '2-digit', minute: '2-digit' }});
      }} else {{
        updated.textContent = 'ожидаем геопозицию мастера';
      }}
      summary.textContent = parts.filter(Boolean).join(' · ');
      timer = setTimeout(refresh, data.order_status === 'en_route' ? 4000 : 8000);
    }} catch (_error) {{
      summary.textContent = 'Связь временно потеряна. Повторяем…';
      timer = setTimeout(refresh, 10000);
    }}
  }}

  refresh();
}})();
</script>
</body>
</html>"""


def render_location_share_page(nonce: str) -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
  <meta name="robots" content="noindex,nofollow,noarchive">
  <title>Передача геопозиции</title>
  <style nonce="{nonce}">
    :root {{ font-family: Inter, system-ui, sans-serif; color-scheme: light; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; min-height: 100dvh; display: grid; place-items: center; padding: 22px; background: #f5f7fa; color: #17202a; }}
    main {{ width: min(100%, 480px); padding: 26px; border-radius: 22px; background: white; box-shadow: 0 14px 50px #102a4320; }}
    h1 {{ margin: 0 0 12px; font-size: 27px; }}
    p {{ color: #52606d; line-height: 1.5; }}
    button {{ width: 100%; min-height: 52px; margin-top: 12px; border: 0; border-radius: 14px; font-size: 17px; font-weight: 700; cursor: pointer; }}
    #start {{ background: #1769e0; color: white; }}
    #stop {{ display: none; background: #e8edf3; color: #17202a; }}
    #status[data-active="true"] {{ color: #087f5b; }}
  </style>
</head>
<body>
<main>
  <h1>Передача геопозиции</h1>
  <p>Оставьте эту страницу открытой во время поездки. Координаты доступны только клиенту по отдельной ссылке и перестанут приниматься после закрытия заявки.</p>
  <p id="status">Нажмите кнопку и разрешите доступ к геопозиции.</p>
  <button id="start" type="button">Начать передачу</button>
  <button id="stop" type="button">Остановить на этом устройстве</button>
</main>
<script nonce="{nonce}">
(() => {{
  'use strict';
  const token = decodeURIComponent(location.hash.slice(1));
  const status = document.getElementById('status');
  const start = document.getElementById('start');
  const stop = document.getElementById('stop');
  let watchId = null;
  let lastSentAt = 0;
  let sending = false;

  function eventId() {{
    if (globalThis.crypto && typeof globalThis.crypto.randomUUID === 'function') return globalThis.crypto.randomUUID();
    return Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);
  }}

  function end(message) {{
    if (watchId !== null) navigator.geolocation.clearWatch(watchId);
    watchId = null;
    status.dataset.active = 'false';
    status.textContent = message;
    start.style.display = 'block';
    stop.style.display = 'none';
  }}

  async function submit(position) {{
    const now = Date.now();
    if (sending || now - lastSentAt < 5000) return;
    sending = true;
    try {{
      const response = await fetch('/v1/public/location', {{
        method: 'POST',
        cache: 'no-store',
        credentials: 'omit',
        headers: {{
          'Content-Type': 'application/json',
          'X-Location-Token': token
        }},
        body: JSON.stringify({{
          latitude: position.coords.latitude,
          longitude: position.coords.longitude,
          accuracy_m: position.coords.accuracy,
          captured_at: new Date(position.timestamp).toISOString(),
          event_id: eventId()
        }})
      }});
      if (!response.ok) {{
        end(response.status === 404 ? 'Заявка завершена или ссылка недействительна.' : 'Сервер временно не принял координаты.');
        return;
      }}
      lastSentAt = now;
      status.dataset.active = 'true';
      status.textContent = 'Геопозиция передаётся · ' + new Date().toLocaleTimeString('ru-RU', {{ hour: '2-digit', minute: '2-digit' }});
    }} catch (_error) {{
      status.textContent = 'Нет связи. Следующая точка будет отправлена автоматически.';
    }} finally {{
      sending = false;
    }}
  }}

  start.addEventListener('click', () => {{
    if (token.length < 43) {{
      end('Ссылка недействительна.');
      return;
    }}
    if (!('geolocation' in navigator)) {{
      end('Это устройство не поддерживает геопозицию.');
      return;
    }}
    start.style.display = 'none';
    stop.style.display = 'block';
    status.textContent = 'Запрашиваем координаты…';
    watchId = navigator.geolocation.watchPosition(
      submit,
      error => end(error.code === 1 ? 'Доступ к геопозиции запрещён.' : 'Не удалось получить координаты.'),
      {{ enableHighAccuracy: true, maximumAge: 5000, timeout: 20000 }}
    );
  }});
  stop.addEventListener('click', () => end('Передача остановлена на этом устройстве.'));
}})();
</script>
</body>
</html>"""
