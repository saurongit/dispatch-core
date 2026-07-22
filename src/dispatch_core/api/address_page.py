# ruff: noqa: E501
from __future__ import annotations


def render_address_page(
    nonce: str,
    *,
    latitude: float,
    longitude: float,
    zoom: int,
) -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
  <meta name="robots" content="noindex,nofollow,noarchive">
  <title>Выбор адреса</title>
  <link rel="stylesheet"
        href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
        integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
        crossorigin="">
  <style nonce="{nonce}">
    :root {{ color-scheme: dark; font-family: Inter, system-ui, sans-serif; --bg:#071426; --panel:#0b1a2f; --blue:#00a3ff; --orange:#ff6b00; }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; min-width:300px; background:var(--bg); color:#f7fafc; }}
    main {{ min-height:100dvh; display:grid; grid-template-rows:auto minmax(390px,1fr) auto; }}
    header {{ padding:18px 16px 14px; }}
    h1 {{ margin:0 0 8px; font-size:clamp(24px,7vw,38px); }}
    p {{ margin:6px 0 0; color:#a9b8c8; line-height:1.45; }}
    .warning {{ padding:10px 12px; border:1px solid #ffb45445; border-radius:12px; background:#ff6b0014; color:#ffd6b8; font-size:13px; }}
    .map-wrap {{ position:relative; min-height:390px; border-block:1px solid #ffffff1a; }}
    #map {{ width:100%; height:100%; min-height:390px; }}
    #mode {{ position:absolute; z-index:1000; left:12px; bottom:24px; max-width:calc(100% - 24px); padding:9px 12px; border-radius:11px; background:#0b1a2fe8; box-shadow:0 8px 24px #0005; font-size:12px; }}
    #panel {{ display:none; padding:14px 16px 18px; background:var(--panel); }}
    label {{ display:block; margin-bottom:7px; color:#c9d4df; font-size:13px; }}
    input {{ width:100%; min-height:48px; padding:11px 13px; border:1px solid #ffffff24; border-radius:12px; background:#ffffff0b; color:white; font-size:16px; }}
    button {{ width:100%; min-height:50px; margin-top:10px; border:0; border-radius:13px; background:var(--orange); color:#101820; font-size:16px; font-weight:800; cursor:pointer; }}
    button:disabled {{ opacity:.55; cursor:wait; }}
    #result {{ min-height:20px; color:#a9b8c8; font-size:13px; }}
    .client-pin {{ position:relative; width:48px; height:48px; }}
    .client-pin::before {{ content:''; position:absolute; inset:0; border:3px solid var(--orange); border-radius:50%; animation:ring 1.5s ease-out infinite; }}
    .client-pin b {{ position:absolute; inset:9px; border:6px solid white; border-radius:50%; background:var(--orange); box-shadow:0 0 22px #ff6b0090; }}
    @keyframes ring {{ 0% {{ transform:scale(.65); opacity:.9; }} 100% {{ transform:scale(1.55); opacity:0; }} }}
    .leaflet-control-attribution {{ font-size:10px; }}
  </style>
</head>
<body>
<main>
  <header>
    <h1>Укажите точку на карте</h1>
    <p class="warning">VPN обычно не меняет GPS, но если точка определяется неверно, вы находитесь не на объекте или включена подмена геолокации — выберите нужное место вручную.</p>
    <p>Первое нажатие разблокирует карту. Передвиньте или увеличьте её, затем нажмите на нужный дом — точка сохранится и карта снова заблокируется.</p>
  </header>
  <div class="map-wrap">
    <div id="map" aria-label="Карта выбора места выполнения работы"></div>
    <div id="mode">🔒 Карта заблокирована — нажмите, чтобы включить перемещение и масштаб</div>
  </div>
  <section id="panel">
    <label for="address">Адрес или ориентир — необязательно</label>
    <input id="address" maxlength="500" autocomplete="street-address" placeholder="Например: улица Ленина, дом 10">
    <button id="save" type="button">Сохранить точку</button>
    <p id="result"></p>
  </section>
</main>
<script nonce="{nonce}"
        src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
        integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
        crossorigin=""></script>
<script nonce="{nonce}">
(() => {{
  'use strict';
  const token = decodeURIComponent(location.hash.slice(1));
  const map = L.map('map', {{ center:[{latitude!r},{longitude!r}], zoom:{zoom}, zoomControl:false, dragging:false, scrollWheelZoom:false, touchZoom:false, doubleClickZoom:false }});
  L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{ maxZoom:19, attribution:'© OpenStreetMap contributors' }}).addTo(map);
  const mode = document.getElementById('mode');
  const panel = document.getElementById('panel');
  const address = document.getElementById('address');
  const save = document.getElementById('save');
  const result = document.getElementById('result');
  const icon = L.divIcon({{ className:'', html:'<div class="client-pin"><b></b></div>', iconSize:[48,48], iconAnchor:[24,24] }});
  let locked = true;
  let marker = null;
  let selected = null;

  function setLocked(value) {{
    locked = value;
    const method = value ? 'disable' : 'enable';
    map.dragging[method]();
    map.scrollWheelZoom[method]();
    map.touchZoom[method]();
    map.doubleClickZoom[method]();
    mode.textContent = value
      ? '🔒 Карта заблокирована — нажмите, чтобы изменить точку'
      : '🔓 Карта активна — переместите её и нажмите на нужный дом';
  }}

  map.on('click', event => {{
    if (locked) {{
      setLocked(false);
      return;
    }}
    selected = event.latlng;
    if (marker === null) marker = L.marker(selected, {{ icon, title:'Место выполнения работы' }}).addTo(map);
    else marker.setLatLng(selected);
    panel.style.display = 'block';
    result.textContent = 'Выбрано: ' + selected.lat.toFixed(5) + ', ' + selected.lng.toFixed(5);
    setLocked(true);
    panel.scrollIntoView({{ behavior:'smooth', block:'nearest' }});
  }});

  save.addEventListener('click', async () => {{
    if (!selected) {{ result.textContent = 'Сначала выберите точку на карте.'; return; }}
    if (token.length < 43) {{ result.textContent = 'Ссылка выбора адреса недействительна.'; return; }}
    save.disabled = true;
    result.textContent = 'Сохраняем точку…';
    try {{
      const response = await fetch('/v1/public/intake/location', {{
        method:'POST', cache:'no-store', credentials:'omit',
        headers:{{ 'Content-Type':'application/json', 'X-Intake-Token':token }},
        body:JSON.stringify({{ latitude:selected.lat, longitude:selected.lng, address:address.value.trim() || null }})
      }});
      if (!response.ok) {{
        result.textContent = response.status === 404 ? 'Ссылка устарела. Вернитесь в бот и запросите адрес заново.' : 'Не удалось сохранить точку. Попробуйте ещё раз.';
        return;
      }}
      const data = await response.json();
      result.textContent = '✅ Точка сохранена: ' + data.address + '. Вернитесь в бот и нажмите «Продолжить после карты».';
      save.style.display = 'none';
      address.disabled = true;
    }} catch (_error) {{
      result.textContent = 'Нет связи с сервером. Попробуйте ещё раз.';
    }} finally {{ save.disabled = false; }}
  }});
}})();
</script>
</body>
</html>"""
