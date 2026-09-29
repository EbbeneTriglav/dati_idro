/*
 * basemaps.js — sfondi cartografici per le mappe di dati_idro (Leaflet 1.9.x)
 *
 * Uso:   <script src="basemaps.js"></script>   (dopo leaflet.js)
 *        addBasemaps(map);                      (al posto del vecchio L.tileLayer CARTO)
 *
 * Opzioni: addBasemaps(map, {
 *            default: 'Scuro (Esri)',   // sfondo iniziale se l'utente non ne ha scelto uno
 *            overlays: {...},           // layer aggiuntivi da mostrare nel selettore
 *            position: 'topright'       // posizione del selettore
 *          });
 *
 * CARTO (facoltativo): CARTO dal 23/09/2026 richiede una chiave. Se ne avete una,
 * definite prima di questo script:  <script>window.CARTO_BASEMAPS_KEY='...';</script>
 * e comparirà anche lo sfondo CARTO. Senza chiave CARTO non viene proposto.
 *
 * La scelta dell'utente viene ricordata nel browser (localStorage).
 * Se lo sfondo attivo non risponde, la mappa passa da sola a OpenStreetMap.
 */
(function () {
  'use strict';

  var ESRI = 'https://server.arcgisonline.com/ArcGIS/rest/services/';
  var ESRI_ATTR = 'Tiles &copy; Esri &mdash; Esri, HERE, Garmin, &copy; OpenStreetMap contributors, GIS user community';
  var STORE_KEY = 'dati_idro_basemap';

  function definitions() {
    var d = {
      'Scuro (Esri)': {
        url: ESRI + 'Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}',
        labels: ESRI + 'Canvas/World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}',
        opt: { attribution: ESRI_ATTR, maxNativeZoom: 16, maxZoom: 19 }
      },
      'Chiaro (Esri)': {
        url: ESRI + 'Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}',
        labels: ESRI + 'Canvas/World_Light_Gray_Reference/MapServer/tile/{z}/{y}/{x}',
        opt: { attribution: ESRI_ATTR, maxNativeZoom: 16, maxZoom: 19 }
      },
      'Topografica (OpenTopoMap)': {
        url: 'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
        opt: {
          attribution: 'Dati &copy; OpenStreetMap contributors, SRTM &mdash; Stile &copy; OpenTopoMap (CC-BY-SA)',
          subdomains: 'abc', maxNativeZoom: 17, maxZoom: 19
        }
      },
      'Topografica (Esri)': {
        url: ESRI + 'World_Topo_Map/MapServer/tile/{z}/{y}/{x}',
        opt: { attribution: ESRI_ATTR, maxZoom: 19 }
      },
      'Stradale (OpenStreetMap)': {
        url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
        opt: { attribution: '&copy; OpenStreetMap contributors', maxZoom: 19 }
      },
      'Satellite (Esri)': {
        url: ESRI + 'World_Imagery/MapServer/tile/{z}/{y}/{x}',
        labels: ESRI + 'Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}',
        opt: { attribution: 'Tiles &copy; Esri &mdash; Esri, Maxar, Earthstar Geographics, GIS user community', maxZoom: 19 }
      }
    };
    var key = window.CARTO_BASEMAPS_KEY;
    if (key) {
      var k = '?key=' + encodeURIComponent(key);
      var cartoAttr = '&copy; OpenStreetMap contributors &copy; CARTO';
      d['Scuro (CARTO)'] = {
        url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png' + k,
        opt: { attribution: cartoAttr, subdomains: 'abcd', maxZoom: 19 }
      };
      d['Chiaro (CARTO)'] = {
        url: 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png' + k,
        opt: { attribution: cartoAttr, subdomains: 'abcd', maxZoom: 19 }
      };
    }
    return d;
  }

  function injectCss() {
    if (document.getElementById('basemaps-css')) return;
    var css =
      '.leaflet-control-layers{background:rgba(17,30,53,.94);color:#e3eaf8;border:1px solid #1e3050!important;border-radius:8px!important}' +
      '.leaflet-control-layers-expanded{padding:8px 12px}' +
      '.leaflet-control-layers label{font:12px system-ui,Arial,sans-serif;margin:3px 0}' +
      '.leaflet-control-layers-separator{border-top-color:#1e3050}' +
      '.leaflet-control-layers-toggle{background-color:#111e35;filter:invert(1) hue-rotate(180deg)}';
    var s = document.createElement('style');
    s.id = 'basemaps-css';
    s.textContent = css;
    document.head.appendChild(s);
  }

  function saved() { try { return localStorage.getItem(STORE_KEY); } catch (e) { return null; } }
  function save(name) { try { localStorage.setItem(STORE_KEY, name); } catch (e) {} }

  window.addBasemaps = function (map, opts) {
    opts = opts || {};
    injectCss();

    if (!map.getPane('basemapLabels')) {
      map.createPane('basemapLabels');
      map.getPane('basemapLabels').style.zIndex = 350;          // sopra lo sfondo, sotto i layer dati
      map.getPane('basemapLabels').style.pointerEvents = 'none';
    }

    var defs = definitions();
    var layers = {};
    var tiles = {};
    Object.keys(defs).forEach(function (name) {
      var def = defs[name];
      var base = L.tileLayer(def.url, def.opt);
      tiles[name] = base;
      if (def.labels) {
        var lab = L.tileLayer(def.labels, {
          pane: 'basemapLabels',
          maxNativeZoom: def.opt.maxNativeZoom || 19,
          maxZoom: 19
        });
        layers[name] = L.layerGroup([base, lab]);
      } else {
        layers[name] = base;
      }
    });

    var start = saved();
    if (!layers[start]) start = opts['default'] && layers[opts['default']] ? opts['default'] : 'Scuro (Esri)';
    layers[start].addTo(map);

    var control = L.control.layers(layers, opts.overlays || {}, {
      position: opts.position || 'topright',
      collapsed: true
    }).addTo(map);

    map.on('baselayerchange', function (e) { save(e.name); });

    // Ripiego automatico: se lo sfondo iniziale non carica nessuna tessera, passa a OpenStreetMap
    var ok = false, errors = 0, fallback = 'Stradale (OpenStreetMap)';
    var first = tiles[start];
    first.on('tileload', function () { ok = true; });
    first.on('tileerror', function () {
      errors++;
      if (!ok && errors >= 6 && start !== fallback) {
        map.removeLayer(layers[start]);
        layers[fallback].addTo(map);
        if (window.console) console.warn('basemaps.js: "' + start + '" non risponde, uso ' + fallback);
      }
    });

    return { layers: layers, control: control };
  };
})();
