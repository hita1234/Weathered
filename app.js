/* =========================================================
   WEATHERED — client-side prototype
   Simulates: Scrapy crawl (#IMD tagged social/citizen posts + public
   datasets) -> AI/ML validation layer (SparkNLP-style hoax/fabrication
   filtering) -> Apache Spark ingest -> Cassandra store -> Django API
   -> this dashboard. All of that pipeline is mocked with static/
   generated data + a "pipeline log" so the flow is visible; a real
   deployment would replace fetchPipelineFeed() with calls to a Django
   REST endpoint backed by Cassandra. See /backend for the real
   Spark/Django/Cassandra skeleton and /docs/ARCHITECTURE.md for the
   schema this UI is modeled on.
   ========================================================= */

const NAV = [
  {id:'home', icon:'🏠'},
  {id:'forecast', icon:'🌦️'},
  {id:'events', icon:'📅'},
  {id:'analytics', icon:'📊'},
  {id:'report', icon:'🚨'},
  {id:'settings', icon:'⚙'},
];

const LANGS = {en:'English', hi:'हिंदी', ta:'தமிழ்', bn:'বাংলা', te:'తెలుగు', mr:'मराठी', gu:'ગુજરાતી', kn:'ಕನ್ನಡ', ml:'മലയാളം', pa:'ਪੰਜਾਬੀ'};

const NAVI18N = {
  en:['Home','Forecast','Events','Analytics','Disaster Reporting','Settings'],
  hi:['होम','पूर्वानुमान','घटनाएं','विश्लेषण','आपदा रिपोर्टिंग','सेटिंग्स'],
  ta:['முகப்பு','முன்னறிவிப்பு','நிகழ்வுகள்','பகுப்பாய்வு','பேரிடர் அறிக்கை','அமைப்புகள்'],
  bn:['হোম','পূর্বাভাস','ইভেন্ট','বিশ্লেষণ','দুর্যোগ রিপোর্টিং','সেটিংস'],
  te:['హోమ్','సూచన','ఈవెంట్స్','విశ్లేషణ','విపత్తు నివేదిక','సెట్టింగ్‌లు'],
  mr:['मुख्यपृष्ठ','अंदाज','घटना','विश्लेषण','आपत्ती अहवाल','सेटिंग्ज'],
  gu:['હોમ','આગાહી','ઘટનાઓ','વિશ્લેષણ','આપત્તિ અહેવાલ','સેટિંગ્સ'],
  kn:['ಮುಖಪುಟ','ಮುನ್ಸೂಚನೆ','ಘಟನೆಗಳು','ವಿಶ್ಲೇಷಣೆ','ವಿಪತ್ತು ವರದಿ','ಸೆಟ್ಟಿಂಗ್‌ಗಳು'],
  ml:['ഹോം','പ്രവചനം','ഇവന്റുകൾ','അനലിറ്റിക്സ്','ദുരന്ത റിപ്പോർട്ടിംഗ്','സെറ്റിംഗ്സ്'],
  pa:['ਹੋਮ','ਪੂਰਵ ਅਨੁਮਾਨ','ਸਮਾਗਮ','ਵਿਸ਼ਲੇਸ਼ਣ','ਆਫ਼ਤ ਰਿਪੋਰਟਿੰਗ','ਸੈਟਿੰਗਾਂ'],
};

const I18N = {
  en:{tagline:"Real-time insights, safer tomorrow", goodMorning:"Good morning", goodAfternoon:"Good afternoon", goodEvening:"Good evening", stay:"Stay informed. Stay safe.",
      safer:"A safer tomorrow", safer2:"Prepared today. Protected tomorrow."},
  hi:{tagline:"सुरक्षित कल के लिए, रीयल-टाइम जानकारी", goodMorning:"शुभ प्रभात", goodAfternoon:"शुभ दोपहर", goodEvening:"शुभ संध्या", stay:"जानकारी रखें। सुरक्षित रहें।",
      safer:"एक सुरक्षित कल", safer2:"आज तैयार। कल सुरक्षित।"},
  ta:{tagline:"பாதுகாப்பான நாளைக்கான நிகழ்நேர தகவல்கள்", goodMorning:"காலை வணக்கம்", goodAfternoon:"மதிய வணக்கம்", goodEvening:"மாலை வணக்கம்", stay:"தகவல் அறிந்திருங்கள். பாதுகாப்பாக இருங்கள்.",
      safer:"பாதுகாப்பான நாளை", safer2:"இன்று தயார். நாளை பாதுகாப்பு."},
  bn:{tagline:"নিরাপদ আগামীর জন্য রিয়েল-টাইম তথ্য", goodMorning:"শুভ সকাল", goodAfternoon:"শুভ অপরাহ্ন", goodEvening:"শুভ সন্ধ্যা", stay:"অবগত থাকুন। নিরাপদ থাকুন।",
      safer:"নিরাপদ আগামীকাল", safer2:"আজ প্রস্তুত। আগামীকাল সুরক্ষিত।"},
  te:{tagline:"సురక్షిత రేపటి కోసం రియల్-టైమ్ సమాచారం", goodMorning:"శుభోదయం", goodAfternoon:"శుభ మధ్యాహ్నం", goodEvening:"శుభ సాయంత్రం", stay:"సమాచారం తెలుసుకోండి. సురక్షితంగా ఉండండి.",
      safer:"సురక్షితమైన రేపు", safer2:"ఈరోజు సిద్ధం. రేపు సురక్షితం."},
  mr:{tagline:"सुरक्षित उद्यासाठी रिअल-टाइम माहिती", goodMorning:"शुभ सकाळ", goodAfternoon:"शुभ दुपार", goodEvening:"शुभ संध्याकाळ", stay:"माहिती ठेवा. सुरक्षित रहा.",
      safer:"सुरक्षित उद्या", safer2:"आज तयार. उद्या सुरक्षित."},
  gu:{tagline:"સુરક્ષિત આવતીકાલ માટે રીઅલ-ટાઇમ માહિતી", goodMorning:"શુભ સવાર", goodAfternoon:"શુભ બપોર", goodEvening:"શુભ સાંજ", stay:"માહિતગાર રહો. સુરક્ષિત રહો.",
      safer:"સુરક્ષિત આવતીકાલ", safer2:"આજે તૈયાર. આવતીકાલે સુરક્ષિત."},
  kn:{tagline:"ಸುರಕ್ಷಿತ ನಾಳೆಗಾಗಿ ರಿಯಲ್-ಟೈಮ್ ಮಾಹಿತಿ", goodMorning:"ಶುಭೋದಯ", goodAfternoon:"ಶುಭ ಮಧ್ಯಾಹ್ನ", goodEvening:"ಶುಭ ಸಂಜೆ", stay:"ಮಾಹಿತಿ ಪಡೆಯಿರಿ. ಸುರಕ್ಷಿತವಾಗಿರಿ.",
      safer:"ಸುರಕ್ಷಿತ ನಾಳೆ", safer2:"ಇಂದು ಸಿದ್ಧ. ನಾಳೆ ಸುರಕ್ಷಿತ."},
  ml:{tagline:"സുരക്ഷിതമായ നാളെയ്ക്കുള്ള തത്സമയ വിവരങ്ങൾ", goodMorning:"സുപ്രഭാതം", goodAfternoon:"ശുഭ ഉച്ച", goodEvening:"ശുഭ സന്ധ്യ", stay:"വിവരമുള്ളവരായിരിക്കുക. സുരക്ഷിതരായിരിക്കുക.",
      safer:"സുരക്ഷിതമായ നാളെ", safer2:"ഇന്ന് തയ്യാർ. നാളെ സുരക്ഷിതം."},
  pa:{tagline:"ਸੁਰੱਖਿਅਤ ਭਲਕ ਲਈ ਰੀਅਲ-ਟਾਈਮ ਜਾਣਕਾਰੀ", goodMorning:"ਸ਼ੁਭ ਸਵੇਰ", goodAfternoon:"ਸ਼ੁਭ ਦੁਪਹਿਰ", goodEvening:"ਸ਼ੁਭ ਸ਼ਾਮ", stay:"ਜਾਣਕਾਰੀ ਰੱਖੋ। ਸੁਰੱਖਿਅਤ ਰਹੋ।",
      safer:"ਸੁਰੱਖਿਅਤ ਭਲਕ", safer2:"ਅੱਜ ਤਿਆਰ। ਭਲਕ ਸੁਰੱਖਿਅਤ।"},
};
let lang = 'en';
let currentPage = 'home';
let searchQuery = '';

const STATES = ["Andhra Pradesh","Arunachal Pradesh","Assam","Bihar","Chhattisgarh","Goa","Gujarat","Haryana",
  "Himachal Pradesh","Jharkhand","Karnataka","Kerala","Madhya Pradesh","Maharashtra","Manipur","Meghalaya",
  "Mizoram","Nagaland","Odisha","Punjab","Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura","Uttar Pradesh",
  "Uttarakhand","West Bengal","Delhi","Jammu and Kashmir","Chandigarh","Puducherry"];

const REAL_LOCATIONS = [
  {city:"New Delhi", state:"Delhi", temp:"28°C", lat:28.61, lon:77.21, region:"North India", elevation:216, terrain:"Inland plains (Yamuna basin)", climate:"Humid subtropical", rainBias:0.25},
  {city:"Mumbai", state:"Maharashtra", temp:"32°C", lat:19.08, lon:72.88, region:"West India", elevation:14, terrain:"Coastal (Arabian Sea)", climate:"Tropical wet & dry", rainBias:0.55},
  {city:"Chennai", state:"Tamil Nadu", temp:"29°C", lat:13.08, lon:80.27, region:"South India", elevation:6, terrain:"Coastal (Bay of Bengal)", climate:"Tropical wet & dry", rainBias:0.5},
  {city:"Chromepet", state:"Tamil Nadu", temp:"29°C", lat:12.9516, lon:80.1462, region:"South India", elevation:16, terrain:"Chennai suburb, Adyar river basin", climate:"Tropical wet & dry", rainBias:0.5},
  {city:"Hyderabad", state:"Telangana", temp:"30°C", lat:17.39, lon:78.49, region:"South India", elevation:542, terrain:"Deccan plateau", climate:"Tropical savanna", rainBias:0.35},
  {city:"Bengaluru", state:"Karnataka", temp:"24°C", lat:12.97, lon:77.59, region:"South India", elevation:920, terrain:"Deccan plateau (elevated)", climate:"Tropical savanna", rainBias:0.4},
  {city:"Kolkata", state:"West Bengal", temp:"31°C", lat:22.57, lon:88.36, region:"East India", elevation:9, terrain:"Ganges delta", climate:"Tropical wet & dry", rainBias:0.45},
  {city:"Ahmedabad", state:"Gujarat", temp:"33°C", lat:23.02, lon:72.57, region:"West India", elevation:53, terrain:"Sabarmati river plain", climate:"Semi-arid", rainBias:0.2},
  {city:"Jaipur", state:"Rajasthan", temp:"34°C", lat:26.91, lon:75.79, region:"North India", elevation:431, terrain:"Edge of the Thar desert", climate:"Semi-arid", rainBias:0.15},
  {city:"Lucknow", state:"Uttar Pradesh", temp:"29°C", lat:26.85, lon:80.95, region:"North India", elevation:123, terrain:"Gangetic plain", climate:"Humid subtropical", rainBias:0.3},
  {city:"Guwahati", state:"Assam", temp:"27°C", lat:26.14, lon:91.73, region:"Northeast India", elevation:55, terrain:"Brahmaputra valley", climate:"Humid subtropical", rainBias:0.5},
  {city:"Bhopal", state:"Madhya Pradesh", temp:"28°C", lat:23.26, lon:77.41, region:"Central India", elevation:527, terrain:"Malwa plateau, lakes", climate:"Humid subtropical", rainBias:0.35},
  {city:"Patna", state:"Bihar", temp:"30°C", lat:25.59, lon:85.14, region:"East India", elevation:53, terrain:"Ganges river plain", climate:"Humid subtropical", rainBias:0.4},
  {city:"Thiruvananthapuram", state:"Kerala", temp:"27°C", lat:8.52, lon:76.94, region:"South India", elevation:16, terrain:"Coastal (Arabian Sea)", climate:"Tropical monsoon", rainBias:0.6},
  {city:"Bhubaneswar", state:"Odisha", temp:"31°C", lat:20.30, lon:85.82, region:"East India", elevation:45, terrain:"Near the Mahanadi delta", climate:"Tropical wet & dry", rainBias:0.45},
  {city:"Raipur", state:"Chhattisgarh", temp:"29°C", lat:21.25, lon:81.63, region:"Central India", elevation:298, terrain:"Chhattisgarh plains", climate:"Tropical wet & dry", rainBias:0.4},
  {city:"Shimla", state:"Himachal Pradesh", temp:"18°C", lat:31.10, lon:77.17, region:"North India", elevation:2205, terrain:"Himalayan foothills", climate:"Subtropical highland", rainBias:0.35},
  {city:"Panaji", state:"Goa", temp:"30°C", lat:15.49, lon:73.82, region:"West India", elevation:12, terrain:"Coastal (Arabian Sea)", climate:"Tropical monsoon", rainBias:0.55},
  {city:"Srinagar", state:"Jammu and Kashmir", temp:"16°C", lat:34.08, lon:74.79, region:"North India", elevation:1585, terrain:"Kashmir valley, Dal Lake", climate:"Humid subtropical (montane)", rainBias:0.25},
];
const HOME_OVERVIEW = REAL_LOCATIONS.slice(0,5);
const HOME_TOP = REAL_LOCATIONS.slice(5,10);

/* ---------- Live user location (was previously a hardcoded, undefined value) ----------
   Defaults to Chromepet, Chennai, then refines via the browser Geolocation API
   (matched to the nearest known city so we can still show real terrain/forecast data
   without calling an external geocoding service — published pages can only reach a
   short allow-list of hosts). */
let userLoc = REAL_LOCATIONS.find(l => l.city === "Chromepet");
let geoStatus = 'locating'; // 'locating' | 'live' | 'denied' | 'unsupported'
let geoAccuracyM = null;
let geoWatchId = null;

function nearestLocation(lat, lon){
  let best = REAL_LOCATIONS[0], bestD = Infinity;
  REAL_LOCATIONS.forEach(l=>{
    const d = Math.hypot(l.lat-lat, l.lon-lon);
    if(d < bestD){ bestD = d; best = l; }
  });
  return best;
}
/* Live location tracking: uses watchPosition (not a one-shot fix) so the
   dashboard keeps following the device as it moves, matched to the nearest
   known city so real terrain/forecast data still applies (published pages
   can only reach a short allow-list of hosts, so no live reverse-geocoding
   API call is made). */
function initUserLocation(){
  if(!navigator.geolocation){ geoStatus = 'unsupported'; if(currentPage==='home') go('home'); return; }
  geoWatchId = navigator.geolocation.watchPosition(
    pos=>{
      geoAccuracyM = Math.round(pos.coords.accuracy);
      geoStatus = 'live';
      userLoc = nearestLocation(pos.coords.latitude, pos.coords.longitude);
      if(currentPage === 'home') go('home'); else refreshLiveHomeBits();
    },
    ()=>{ geoStatus = 'denied'; if(currentPage==='home') go('home'); /* keep the Chromepet, Chennai default */ },
    {enableHighAccuracy:false, timeout:8000, maximumAge:120000}
  );
}

/* ---------- Live clock / greeting (previously hardcoded, so it always showed
   "Good Evening" and a fixed date no matter the real time) ---------- */
function greetingText(t){
  const hour = new Date().getHours(); // local device time
  if(hour >= 4 && hour < 12) return t.goodMorning;
  if(hour >= 12 && hour < 17) return t.goodAfternoon;
  return t.goodEvening; // 17:00–23:59 and 00:00–03:59
}
function fmtISTNow(){
  const opts = {weekday:'short', day:'2-digit', month:'short', year:'numeric', hour:'2-digit', minute:'2-digit', hour12:true, timeZone:'Asia/Kolkata'};
  return new Intl.DateTimeFormat('en-IN', opts).format(new Date()) + ' IST';
}
/* ---------- Live weather intelligence: small realistic fluctuation layer ----------
   A real deployment streams live sensor/model updates from the Django API;
   here each tick nudges temp/humidity/wind by a small deterministic-random
   delta around the city's baseline, so numbers visibly *live-update* on
   Home instead of sitting frozen, while staying anchored to reality. */
let liveTick = 0;
function liveWeatherSnapshot(loc){
  const key = cityKey(loc.city) + liveTick*3.3;
  const base = parseInt(loc.temp,10);
  const temp = Math.round((base + (seedRand(key)-0.5)*1.6)*10)/10;
  const humidity = Math.round(40 + loc.rainBias*40 + seedRand(key+21)*8);
  const wind = Math.round(8 + seedRand(key+42)*10);
  return {temp, humidity, wind};
}
function refreshLiveHomeBits(){
  if(currentPage !== 'home') return;
  liveTick++;
  const t = I18N[lang];
  const g = document.getElementById('homeGreeting');
  if(g) g.textContent = `${greetingText(t)}, Bharat`;
  const dt = document.getElementById('homeDateTime');
  if(dt) dt.textContent = `📅 ${fmtISTNow()}`;
  const loc = document.getElementById('homeLocation');
  if(loc) loc.textContent = `📍 ${userLoc.city}, ${userLoc.state}`;
  const snap = liveWeatherSnapshot(userLoc);
  const temp = document.getElementById('homeTemp');
  if(temp) temp.textContent = `🌤 ${snap.temp}°C`;
  const hum = document.getElementById('homeHumidity');
  if(hum) hum.textContent = `💧 ${snap.humidity}%`;
  const wind = document.getElementById('homeWind');
  if(wind) wind.textContent = `🌬 ${snap.wind} km/h`;
  const fresh = document.getElementById('homeFreshness');
  if(fresh) fresh.textContent = `Updated just now`;
}
/* Clock/date tick every second; full live-weather nudge every 8s (feels
   "live" without being a distracting flicker on every number). */
setInterval(()=>{
  if(currentPage !== 'home') return;
  const dt = document.getElementById('homeDateTime');
  if(dt) dt.textContent = `📅 ${fmtISTNow()}`;
}, 1000);
setInterval(refreshLiveHomeBits, 8000);

/* ---------- Forecast engine (deterministic per city, so it doesn't reshuffle on every render) ---------- */
const COND_ICON = {Sunny:'☀️','Partly Cloudy':'⛅',Cloudy:'☁️','Light Rain':'🌦️','Heavy Rain':'🌧️',Thunderstorm:'⛈️'};
const WD = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
function seedRand(seed){ const x = Math.sin(seed)*10000; return x - Math.floor(x); }
function cityKey(city){ let h=7; for(let i=0;i<city.length;i++) h=(h*31+city.charCodeAt(i))%99991; return h; }
function genForecast(loc){
  const base = parseInt(loc.temp,10), key = cityKey(loc.city), today = new Date(), out=[];
  for(let i=0;i<7;i++){
    const r1=seedRand(key+i*13.7), r2=seedRand(key+i*29.3+5), rr=seedRand(key+i*7.1+11);
    const hi = Math.round(base + (r1-0.5)*6), lo = Math.round(hi - 5 - r2*4);
    let cond;
    if(rr < loc.rainBias*0.15) cond='Thunderstorm';
    else if(rr < loc.rainBias*0.55) cond='Heavy Rain';
    else if(rr < loc.rainBias) cond='Light Rain';
    else if(rr < loc.rainBias+0.25) cond='Cloudy';
    else if(rr < loc.rainBias+0.5) cond='Partly Cloudy';
    else cond='Sunny';
    const rainPct = Math.max(5, Math.min(95, Math.round(loc.rainBias*100 + (r1-0.5)*20 + (cond==='Thunderstorm'?30:cond==='Heavy Rain'?18:cond==='Light Rain'?8:0))));
    const dt = new Date(today); dt.setDate(today.getDate()+i);
    out.push({label: i===0?'Today':WD[dt.getDay()], date:`${dt.getDate()} ${MONTHS[dt.getMonth()+1]}`, hi, lo, cond, icon:COND_ICON[cond], rain:rainPct});
  }
  return out;
}

/* ---------- Stylised India outline map for showing a city's real coordinates ---------- */
const INDIA_PATH = "M250,18 L282,36 L305,58 L332,78 L360,100 L386,128 L378,155 L352,168 L368,192 L386,222 L376,252 L356,288 L342,326 L328,364 L308,404 L288,440 L264,472 L238,498 L214,476 L196,444 L184,406 L176,366 L166,326 L156,288 L146,250 L128,216 L104,204 L90,178 L108,150 L136,130 L154,98 L172,74 L196,52 L222,32 Z";
const MAP_BOUNDS = {latMin:8, latMax:35.5, lonMin:68, lonMax:97.5, xMin:90, xMax:386, yMin:18, yMax:498};
function mapXY(lat,lon){
  const b = MAP_BOUNDS;
  const x = b.xMin + (lon-b.lonMin)/(b.lonMax-b.lonMin)*(b.xMax-b.xMin);
  const y = b.yMax - (lat-b.latMin)/(b.latMax-b.latMin)*(b.yMax-b.yMin);
  return [x,y];
}
function indiaMapSVG(loc){
  const [x,y] = mapXY(loc.lat, loc.lon);
  const dots = REAL_LOCATIONS.filter(l=>l.city!==loc.city).map(l=>{ const [dx,dy]=mapXY(l.lat,l.lon); return `<circle cx="${dx.toFixed(1)}" cy="${dy.toFixed(1)}" r="2.4" fill="var(--ink-soft)" opacity="0.35"/>`; }).join('');
  return `<svg viewBox="0 0 480 520" width="100%" height="280" role="img" aria-label="Map of India showing ${loc.city}">
    <path d="${INDIA_PATH}" fill="var(--teal-soft)" stroke="var(--teal)" stroke-width="2" stroke-linejoin="round"/>
    ${dots}
    <circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="10" fill="var(--terracotta)" opacity="0.22"><animate attributeName="r" values="8;14;8" dur="2s" repeatCount="indefinite"/></circle>
    <circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="5.5" fill="var(--terracotta)" stroke="#fff" stroke-width="1.5"/>
    <text x="${x.toFixed(1)}" y="${(y-12).toFixed(1)}" text-anchor="middle" font-size="13" font-weight="700" fill="var(--ink)">${loc.city}</text>
  </svg>`;
}

/* ---------- Mock "crawled + validated" data, anchored to real Indian cities/states ---------- */
const EVENTS = [
  {iso:"2026-09-25", time:"16:20", type:"Heavy Rainfall", city:"Chennai", state:"Tamil Nadu", sev:"high", status:"ongoing", src:"IMD"},
  {iso:"2026-09-25", time:"12:10", type:"Cyclone Alert", city:"Visakhapatnam", state:"Andhra Pradesh", sev:"high", status:"ongoing", src:"IMD"},
  {iso:"2026-09-25", time:"10:45", type:"Heatwave", city:"Jaipur", state:"Rajasthan", sev:"med", status:"ongoing", src:"IMD"},
  {iso:"2026-09-24", time:"22:30", type:"Flood Alert", city:"Guwahati", state:"Assam", sev:"med", status:"resolved", src:"State Agency"},
  {iso:"2026-09-24", time:"18:15", type:"Strong Winds", city:"Ahmedabad", state:"Gujarat", sev:"low", status:"resolved", src:"IMD"},
  {iso:"2026-09-23", time:"09:05", type:"Landslide Warning", city:"Ooty", state:"Tamil Nadu", sev:"high", status:"ongoing", src:"Citizen + IMD"},
  {iso:"2026-09-22", time:"14:00", type:"Thunderstorm Warning", city:"Bhopal", state:"Madhya Pradesh", sev:"med", status:"resolved", src:"IMD"},
  {iso:"2026-09-21", time:"08:30", type:"Coastal Erosion Watch", city:"Panaji", state:"Goa", sev:"low", status:"ongoing", src:"IMD"},
  {iso:"2026-09-20", time:"19:45", type:"Heavy Rainfall", city:"Kochi", state:"Kerala", sev:"high", status:"ongoing", src:"IMD"},
  {iso:"2026-09-19", time:"11:20", type:"Drought Watch", city:"Latur", state:"Maharashtra", sev:"med", status:"ongoing", src:"State Agency"},
  {iso:"2026-09-18", time:"07:15", type:"Snowfall Alert", city:"Shimla", state:"Himachal Pradesh", sev:"low", status:"resolved", src:"IMD"},
  {iso:"2026-09-15", time:"13:00", type:"Flash Flood", city:"Patna", state:"Bihar", sev:"high", status:"resolved", src:"IMD"},
  {iso:"2026-09-10", time:"09:00", type:"Heatwave", city:"Bhubaneswar", state:"Odisha", sev:"med", status:"resolved", src:"IMD"},
  {iso:"2026-08-29", time:"17:00", type:"Cyclone Watch", city:"Puri", state:"Odisha", sev:"high", status:"resolved", src:"IMD"},
  {iso:"2026-07-20", time:"06:40", type:"Monsoon Onset Alert", city:"Mumbai", state:"Maharashtra", sev:"low", status:"resolved", src:"IMD"},
];
function categorize(type){
  const t = type.toLowerCase();
  if(t.includes('flood')) return 'Flood';
  if(t.includes('cyclone')) return 'Cyclone';
  if(t.includes('heatwave')) return 'Heatwave';
  if(t.includes('landslide')) return 'Landslide';
  if(t.includes('rain')||t.includes('wind')||t.includes('thunderstorm')||t.includes('snow')||t.includes('drought')||t.includes('erosion')||t.includes('monsoon')) return 'Weather';
  return 'Other';
}
function sevLabel(s){ return s==='med' ? 'Medium' : s.charAt(0).toUpperCase()+s.slice(1); }

const ALERTS = EVENTS.filter(e=>e.status==='ongoing').slice(0,5).map(e=>
  ({sev:e.sev, t:`${e.type} — ${e.city}, ${e.state}`, time:e.time}));

const TREND = [40,52,48,63,70,58,91]; // % rainfall-linked events, last 7 days
function computeStateDist(){
  const counts={};
  EVENTS.forEach(e=>{ counts[e.state]=(counts[e.state]||0)+1; });
  return Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([s,n])=>({s,n}));
}
function computeTypeDist(){
  const counts={};
  EVENTS.forEach(e=>{ const c=categorize(e.type); counts[c]=(counts[c]||0)+1; });
  const palette=['var(--teal)','var(--terracotta)','var(--saffron)','#8A6D3B','#9AA6BD'];
  return Object.entries(counts).map(([t,n],i)=>({t,n,c:palette[i%palette.length]}));
}
const INSIGHTS = [
  "Rainfall activity has picked up sharply across South India in the last 7 days.",
  "Heatwave conditions remain likely in Rajasthan and Odisha over the next 2 days.",
  "Cyclone risk remains high near the Andhra Pradesh and Odisha coastline.",
  "91% rainfall recorded in Chennai over the past week — highest since June.",
];

/* pipeline log shown on Report page to make the crawl->validate->store flow visible */
function pipelineLog(reportId){
  return [
    `Scrapy crawler picked up #IMD-tagged post referencing ${reportId}`,
    "AI/ML validation layer (SparkNLP) scored the report: no hoax/fabrication signals",
    "Apache Spark job ingested the validated record",
    "Record written to Apache Cassandra via Django API",
    "Acknowledgement issued to State Disaster Management Authority"
  ];
}

/* ---------- State ---------- */
let online = navigator.onLine;
let lastSyncTime = new Date();
let queue = JSON.parse(localStorage.getItem('wx_queue')||'[]');
let reportDraft = {type:'', desc:'', state:'', district:'', files:[]};
let reportStep = 1;

function saveQueue(){ localStorage.setItem('wx_queue', JSON.stringify(queue)); }

function toast(msg){
  const el = document.getElementById('toast');
  el.textContent = msg; el.style.display='block';
  clearTimeout(window._toastT);
  window._toastT = setTimeout(()=>el.style.display='none', 2600);
}

/* ---------- Nav & routing ---------- */
function buildNav(){
  const nav = document.getElementById('nav');
  nav.innerHTML = '';
  NAV.forEach((item,i)=>{
    const b = document.createElement('button');
    b.innerHTML = `<span>${item.icon}</span><span>${NAVI18N[lang][i]}</span>`;
    b.dataset.id = item.id;
    b.onclick = ()=> go(item.id);
    nav.appendChild(b);
  });
}
function setActiveNav(id){
  document.querySelectorAll('#nav button').forEach(b=> b.classList.toggle('active', b.dataset.id===id));
}
function fmtTime(d){ return d.toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',second:'2-digit'}); }
function offlineBanner(){
  return `<div class="warn">⚠ You're offline — showing data synced up to <b>${fmtTime(lastSyncTime)}</b>, the moment connectivity was lost. Live dashboards and the Forecast page pause until you're back online. Any disaster report you submit now is queued and will send itself automatically the moment you're reconnected.</div>`;
}
function go(id){
  currentPage = id;
  setActiveNav(id);
  const c = document.getElementById('content');
  c.innerHTML = (online ? '' : offlineBanner()) + RENDER[id]();
  wirePage(id);
  location.hash = id;
}

/* ---------- Renderers ---------- */
function statCard(label,val,delta,down){
  return `<div class="stat"><div class="label">${label}</div><div class="val">${val}</div>
    <div class="delta ${down?'down':''}">${down?'↓':'↑'} ${delta}</div></div>`;
}
function svgLine(vals, color){
  const w=280,h=90,max=Math.max(...vals),min=Math.min(...vals);
  const pts = vals.map((v,i)=> `${(i/(vals.length-1))*w},${h-((v-min)/(max-min||1))*(h-16)-8}`).join(' ');
  return `<svg viewBox="0 0 ${w} ${h}" width="100%" height="${h}">
    <polyline points="${pts}" fill="none" stroke="${color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
    ${vals.map((v,i)=>{const [x,y]=pts.split(' ')[i].split(',');return `<circle cx="${x}" cy="${y}" r="3" fill="${color}"/>`}).join('')}
  </svg>`;
}
function donut(dist){
  const total = dist.reduce((a,b)=>a+b.n,0);
  let acc=0; const r=42,cx=54,cy=54,circ=2*Math.PI*r;
  const segs = dist.map(d=>{
    const frac=d.n/total; const dash=frac*circ;
    const seg=`<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${d.c}" stroke-width="16"
      stroke-dasharray="${dash} ${circ-dash}" stroke-dashoffset="${-acc}" transform="rotate(-90 ${cx} ${cy})"/>`;
    acc += dash; return seg;
  }).join('');
  return `<svg viewBox="0 0 108 108" width="110" height="110">${segs}
    <text x="54" y="50" text-anchor="middle" font-size="16" font-weight="700" fill="var(--ink)">${total}</text>
    <text x="54" y="65" text-anchor="middle" font-size="9" fill="var(--ink-soft)">events</text></svg>`;
}
function pieSVG(dist){
  const total = dist.reduce((a,b)=>a+b.n,0);
  let acc=0; const R=44,cx=54,cy=54,circ=2*Math.PI*(R/2);
  const segs = dist.map(d=>{
    const frac=d.n/total; const dash=frac*circ;
    const seg=`<circle cx="${cx}" cy="${cy}" r="${R/2}" fill="none" stroke="${d.c}" stroke-width="${R}"
      stroke-dasharray="${dash} ${circ-dash}" stroke-dashoffset="${-acc}" transform="rotate(-90 ${cx} ${cy})"/>`;
    acc += dash; return seg;
  }).join('');
  return `<svg viewBox="0 0 108 108" width="110" height="110">${segs}</svg>`;
}
function stackedBarSVG(){
  const { matrix, cats } = computeTypeSeverityMatrix();
  const totals = cats.map(c=> matrix[c].high+matrix[c].med+matrix[c].low);
  const max = Math.max(...totals,1), w=300,h=150,gap=w/cats.length,bw=gap*0.5;
  const sevs = ['low','med','high'];
  const bars = cats.map((c,i)=>{
    const x=i*gap+(gap-bw)/2; let yCursor=h-24;
    const segs = sevs.map(s=>{
      const v=matrix[c][s]||0, bh=(v/max)*(h-46);
      const y=yCursor-bh; yCursor-=bh;
      return v ? `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" fill="${SEV_COLOR[s]}"/>` : '';
    }).join('');
    const total = matrix[c].high+matrix[c].med+matrix[c].low;
    return `${segs}
      <text x="${(x+bw/2).toFixed(1)}" y="${(h-24-(total/max)*(h-46)-4).toFixed(1)}" font-size="10" text-anchor="middle" fill="var(--ink)">${total}</text>
      <text x="${(x+bw/2).toFixed(1)}" y="${h-8}" font-size="10" text-anchor="middle" fill="var(--ink-soft)">${c}</text>`;
  }).join('');
  return `<svg viewBox="0 0 ${w} ${h}" width="100%" height="${h}">${bars}</svg>
    <div style="display:flex;gap:14px;font-size:11.5px;margin-top:6px">
      ${sevs.map(s=>`<span style="display:flex;align-items:center;gap:5px"><span style="width:9px;height:9px;border-radius:2px;background:${SEV_COLOR[s]};display:inline-block"></span>${sevLabel(s)}</span>`).join('')}
    </div>`;
}
function radarStateEventSVG(){
  const { states, cats, m } = computeStateEventMatrix(3);
  const n = cats.length, R=62, cx=90, cy=88;
  const maxVal = Math.max(1, ...states.flatMap(s=>cats.map(c=>m[s][c])));
  const pt=(i,val)=>{ const ang=(Math.PI*2*i/n)-Math.PI/2, r=(val/maxVal)*R; return [cx+r*Math.cos(ang), cy+r*Math.sin(ang)]; };
  const axis = cats.map((c,i)=>{ const [x,y]=pt(i,maxVal); return `<line x1="${cx}" y1="${cy}" x2="${x.toFixed(1)}" y2="${y.toFixed(1)}" stroke="var(--line)"/>`; }).join('');
  const rings = [0.34,0.67,1].map(f=>`<polygon points="${cats.map((c,i)=>{const [x,y]=pt(i,maxVal*f);return `${x.toFixed(1)},${y.toFixed(1)}`;}).join(' ')}" fill="none" stroke="var(--line)"/>`).join('');
  const polys = states.map((s,si)=>{
    const color = STATE_PALETTE[si%STATE_PALETTE.length];
    const poly = cats.map((c,i)=>{ const [x,y]=pt(i,m[s][c]); return `${x.toFixed(1)},${y.toFixed(1)}`; }).join(' ');
    return `<polygon points="${poly}" fill="${color}" fill-opacity="0.18" stroke="${color}" stroke-width="2"/>`;
  }).join('');
  const labels = cats.map((c,i)=>{ const [x,y]=pt(i,maxVal*1.26); return `<text x="${x.toFixed(1)}" y="${y.toFixed(1)}" font-size="10" text-anchor="middle" fill="var(--ink-soft)">${c}</text>`; }).join('');
  return `<svg viewBox="0 0 180 176" width="100%" height="210">${rings}${axis}${polys}${labels}</svg>
    <div style="display:flex;gap:14px;font-size:11.5px;flex-wrap:wrap">
      ${states.map((s,i)=>`<span style="display:flex;align-items:center;gap:5px"><span style="width:9px;height:9px;border-radius:50%;background:${STATE_PALETTE[i%STATE_PALETTE.length]};display:inline-block"></span>${s}</span>`).join('')}
    </div>`;
}
const SEV_COLOR = {high:'var(--terracotta)', med:'var(--saffron)', low:'var(--teal)'};
const STATE_PALETTE = ['var(--teal)','var(--terracotta)','var(--saffron)','#6B4FA0','#3E7CB1'];

function computeTypeSeverityMatrix(){
  const cats = ['Weather','Flood','Cyclone','Heatwave','Landslide','Other'];
  const m = {}; cats.forEach(c=> m[c]={high:0,med:0,low:0});
  EVENTS.forEach(e=>{ const c=categorize(e.type); m[c][e.sev]++; });
  return { matrix:m, cats: cats.filter(c=> m[c].high+m[c].med+m[c].low>0) };
}
function computeStateEventMatrix(topN=5){
  const states = computeStateDist().slice(0,topN).map(s=>s.s);
  const cats = computeTypeSeverityMatrix().cats;
  const m = {}; states.forEach(s=> m[s]=Object.fromEntries(cats.map(c=>[c,0])));
  EVENTS.forEach(e=>{ if(states.includes(e.state)){ const c=categorize(e.type); if(cats.includes(c)) m[e.state][c]++; } });
  return { states, cats, m };
}
function computeStatePieDist(){
  return computeStateDist().map((d,i)=>({t:d.s, n:d.n, c:STATE_PALETTE[i%STATE_PALETTE.length]}));
}
function heatmapStateEvent(){
  const { states, cats, m } = computeStateEventMatrix(5);
  const max = Math.max(...states.flatMap(s=>cats.map(c=>m[s][c])), 1);
  const cell = (s,c)=>{ const v=m[s][c]||0, alpha = v ? (0.18+0.72*(v/max)) : 0.06;
    return `<td style="background:rgba(29,120,116,${alpha});text-align:center;font-weight:700;color:${v?'#fff':'var(--ink-soft)'}">${v||'—'}</td>`; };
  return `<table><thead><tr><th>State</th>${cats.map(c=>`<th>${c}</th>`).join('')}</tr></thead>
    <tbody>${states.map(s=>`<tr><td style="font-weight:600">${s}</td>${cats.map(c=>cell(s,c)).join('')}</tr>`).join('')}</tbody></table>`;
}

RENDER = {};

RENDER.home = ()=>{
  const t = I18N[lang];
  const snap = liveWeatherSnapshot(userLoc);
  return `
  <div class="hero">
    <div class="live-pill"><span class="live-dot"></span>LIVE</div>
    <h1 id="homeGreeting">${greetingText(t)}, Bharat</h1>
    <p>${t.stay}</p>
    <div class="meta">
      <span id="homeDateTime">📅 ${fmtISTNow()}</span>
      <span id="homeLocation">📍 ${userLoc.city}, ${userLoc.state}</span>
      <span id="homeTemp">🌤 ${snap.temp}°C</span>
      <span id="homeHumidity">💧 ${snap.humidity}%</span>
      <span id="homeWind">🌬 ${snap.wind} km/h</span>
    </div>
    <div class="meta" style="margin-top:6px;opacity:.85">
      <span id="homeFreshness" style="font-size:12px">Updated just now</span>
    </div>
  </div>
  <div class="stats">
    ${statCard('Total Events','1,248','12%')}
    ${statCard('Verified','842','16%')}
    ${statCard('Pending Review','217','8%')}
    ${statCard('Active Alerts','125','5%',true)}
  </div>
  <div class="grid2">
    <div class="card"><h3>Weather Overview</h3>
      ${HOME_OVERVIEW.map(l=>`<div class="loc-row"><span>${l.city}, ${l.state}</span><span>${l.temp}</span></div>`).join('')}
    </div>
    <div class="card"><h3>Top Weather Alerts <span style="float:right;font-size:11px;color:var(--ink-soft);font-weight:400">live feed</span></h3>
      ${ALERTS.map(a=>`<div class="alert-row"><span class="sev ${a.sev}"></span><div><div>${a.t}</div><div style="color:var(--ink-soft);font-size:11px">${a.time}</div></div></div>`).join('')}
    </div>
  </div>
  <div class="grid2">
    <div class="card"><h3>Weather Trends — rainfall-linked events (7 days)</h3>${svgLine(TREND,'var(--teal)')}</div>
    <div class="card"><h3>Top Locations</h3>
      ${HOME_TOP.map(l=>`<div class="loc-row"><span>${l.city}, ${l.state}</span><span>${l.temp}</span></div>`).join('')}
    </div>
  </div>
  <div class="hero" style="background:linear-gradient(120deg,var(--teal) 0%,var(--night) 130%)">
    <h2 style="margin:0">${t.safer}</h2><p>${t.safer2}</p>
  </div>`;
};

RENDER.forecast = ()=>{
  if(!online){
    return `
    <h1>Forecast</h1>
    <p style="color:var(--ink-soft);margin-top:-8px">7-day weather outlook and local geography for any city or state in India.</p>
    <div class="card">
      <h3>🔒 Forecast unavailable offline</h3>
      <p style="font-size:13px;color:var(--ink-soft);line-height:1.6">Forecasting needs a live connection to pull the latest model output, so it's switched off while you're offline rather than showing a stale prediction.
      Your last-synced Home, Events and Analytics data (as of ${fmtTime(lastSyncTime)}) is still available from the sidebar. Reconnect to bring Forecast back.</p>
    </div>`;
  }
  const states = [...new Set(REAL_LOCATIONS.map(l=>l.state))].sort();
  return `
  <h1>Forecast</h1>
  <p style="color:var(--ink-soft);margin-top:-8px">7-day weather outlook and local geography for any city or state in India.</p>
  <div class="row" style="margin-bottom:10px">
    <select class="full" id="fcState">${states.map(s=>`<option>${s}</option>`).join('')}</select>
    <select class="full" id="fcCity"></select>
  </div>
  <div class="row" style="margin-bottom:14px">
    <select class="full" id="fcEvent">
      <option value="all">All Conditions</option>
      ${Object.keys(COND_ICON).map(c=>`<option value="${c}">${COND_ICON[c]} ${c}</option>`).join('')}
    </select>
    <select class="full" id="fcDate"><option value="all">All 7 Days</option></select>
  </div>
  <div id="fcBody">Loading forecast…</div>`;
};
const TIME_BLOCKS = [
  {label:'Early Morning', icon:'🌅', frac:0.15},
  {label:'Afternoon', icon:'☀️', frac:0.9},
  {label:'Evening', icon:'🌇', frac:0.55},
  {label:'Night', icon:'🌙', frac:0.22},
];
function genHourly(loc, dayEntry, dayIdx){
  const key = cityKey(loc.city) + dayIdx*17.3;
  return TIME_BLOCKS.map((b,i)=>{
    const r = seedRand(key + i*9.1 + 3);
    const temp = Math.round(dayEntry.lo + (dayEntry.hi-dayEntry.lo)*b.frac + (r-0.5)*2);
    return {label:b.label, icon:b.icon, temp};
  });
}
function forecastBodyHTML(loc, eventType='all', dateIdx='all'){
  const fc = genForecast(loc), key = cityKey(loc.city);
  const filtered = fc.filter((d,i)=>
    (eventType==='all' || d.cond===eventType) && (dateIdx==='all' || String(i)===String(dateIdx)));
  const focus = filtered[0] || fc[0];
  const humidity = Math.round(40 + loc.rainBias*40 + seedRand(key+99)*10);
  const wind = Math.round(8 + seedRand(key+55)*12);
  const uv = Math.min(11, Math.round(4 + seedRand(key+77)*6));
  const filterActive = eventType!=='all' || dateIdx!=='all';
  const heroLabel = dateIdx==='all' ? (focus.label==='Today'?'today':`on ${focus.label}, ${focus.date}`) : `on ${focus.label}, ${focus.date}`;
  const strip = filtered.length ? filtered : fc;
  const dayIdxOf = d => fc.indexOf(d);
  return `
  <div class="hero" style="background:linear-gradient(120deg,var(--teal) 0%,var(--night) 140%)">
    <h1>${loc.city}, ${loc.state}</h1>
    <p>${filtered.length ? `${focus.icon} ${focus.cond} · ${focus.hi}°C / ${focus.lo}°C ${heroLabel}` : `No matching days — showing overview`}</p>
    <div class="meta"><span>💧 Humidity ${humidity}%</span><span>🌬 Wind ${wind} km/h</span><span>☀ UV Index ${uv}</span><span>📍 ${loc.lat.toFixed(2)}°N, ${loc.lon.toFixed(2)}°E</span></div>
  </div>
  ${dateIdx!=='all' && filtered.length ? `
  <div class="card" style="margin-bottom:16px"><h3>Time of Day — ${focus.label}, ${focus.date}</h3>
    <div class="row">
      ${genHourly(loc, focus, dayIdxOf(focus)).map(b=>`
        <div style="flex:1;min-width:110px;text-align:center;background:var(--paper);border:1px solid var(--line);border-radius:10px;padding:12px 6px">
          <div style="font-size:12px;font-weight:600">${b.label}</div>
          <div style="font-size:22px;margin:6px 0">${b.icon}</div>
          <div style="font-size:13px">${b.temp}°C</div>
        </div>`).join('')}
    </div>
  </div>` : ''}
  <div class="card" style="margin-bottom:16px"><h3>${filterActive ? `Matching Days (${filtered.length}/7)` : '7-Day Forecast'}</h3>
    ${filtered.length ? `<div style="display:flex;gap:8px;overflow-x:auto;padding-bottom:4px">
      ${strip.map(d=>`<div style="flex:0 0 86px;text-align:center;background:var(--paper);border:1px solid var(--line);border-radius:10px;padding:10px 6px">
        <div style="font-size:12px;font-weight:600">${d.label}</div>
        <div style="font-size:11px;color:var(--ink-soft)">${d.date}</div>
        <div style="font-size:22px;margin:6px 0">${d.icon}</div>
        <div style="font-size:12.5px">${d.hi}° / ${d.lo}°</div>
        <div style="font-size:11px;color:var(--teal)">💧 ${d.rain}%</div>
      </div>`).join('')}
    </div>` : `<p style="font-size:13px;color:var(--ink-soft)">No days in the next 7 match <b>${eventType}</b> for ${loc.city}. Try a different condition or date.</p>`}
  </div>
  <div class="grid2">
    <div class="card"><h3>Where this is on the map</h3>
      ${indiaMapSVG(loc)}
      <footer class="note">Plotted from the city's real latitude/longitude on a simplified outline of India (published pages can't load live map tiles, so this stays fully offline-safe).</footer>
    </div>
    <div class="card"><h3>Geographical Details</h3>
      <div class="loc-row"><span>State</span><span>${loc.state}</span></div>
      <div class="loc-row"><span>Region</span><span>${loc.region}</span></div>
      <div class="loc-row"><span>Coordinates</span><span>${loc.lat.toFixed(2)}°N, ${loc.lon.toFixed(2)}°E</span></div>
      <div class="loc-row"><span>Elevation</span><span>${loc.elevation} m</span></div>
      <div class="loc-row"><span>Terrain</span><span>${loc.terrain}</span></div>
      <div class="loc-row"><span>Climate zone</span><span>${loc.climate}</span></div>
    </div>
  </div>
  <div class="card" style="margin-top:16px"><h3>Weekly High Temperature Trend (°C)</h3>${svgLine(fc.map(d=>d.hi),'var(--saffron-deep)')}</div>`;
}

let evFilter = 'All Events';
RENDER.events = ()=>{
  return `
  <h1>Events</h1>
  <p style="color:var(--ink-soft);margin-top:-8px">View and track weather and disaster events across regions.</p>
  <div class="tabs" id="evTabs">
    ${['All Events','Weather','Flood','Cyclone','Heatwave','Landslide','Other'].map(x=>`<button class="${x===evFilter?'active':''}" data-f="${x}">${x}</button>`).join('')}
  </div>
  <div class="row" style="margin-bottom:14px">
    <select class="full" id="evDateRange"><option value="All Time">All Time</option><option value="Last 7 Days">Last 7 Days</option><option value="Last 30 Days">Last 30 Days</option></select>
    <select class="full" id="evState"><option>All States</option>${STATES.map(s=>`<option>${s}</option>`).join('')}</select>
    <select class="full" id="evSeverity"><option value="All Severity">All Severity</option><option value="high">High</option><option value="med">Medium</option><option value="low">Low</option></select>
  </div>
  <div class="card">
  <table id="evTable"><thead><tr><th>Date/Time</th><th>Event</th><th>Location</th><th>Severity</th><th>Status</th><th>Source</th></tr></thead>
  <tbody id="evTableBody"></tbody>
  </table>
  </div>`;
};
const MONTHS = ['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function fmtDate(iso){ const [y,m,d] = iso.split('-'); return `${d} ${MONTHS[+m]} ${y}`; }
function evRowHTML(e){
  return `<tr><td>${fmtDate(e.iso)}, ${e.time}</td>
  <td>${e.type}</td><td>${e.city}, ${e.state}</td>
  <td><span class="badge ${e.sev}">${sevLabel(e.sev)}</span></td>
  <td><span class="badge ${e.status}">${e.status==='ongoing'?'Ongoing':'Resolved'}</span></td><td>${e.src}</td></tr>`;
}
function applyEventFilters(){
  const stateVal = document.getElementById('evState') ? document.getElementById('evState').value : 'All States';
  const sevVal = document.getElementById('evSeverity') ? document.getElementById('evSeverity').value : 'All Severity';
  const dateVal = document.getElementById('evDateRange') ? document.getElementById('evDateRange').value : 'All Time';
  const q = searchQuery.trim().toLowerCase();
  const today = new Date();
  const rows = EVENTS.filter(e=>{
    if(evFilter!=='All Events' && categorize(e.type)!==evFilter) return false;
    if(stateVal!=='All States' && e.state!==stateVal) return false;
    if(sevVal!=='All Severity' && e.sev!==sevVal) return false;
    const dt = new Date(e.iso+'T'+e.time);
    const diffDays = (today - dt)/86400000;
    if(dateVal==='Last 7 Days' && diffDays>7) return false;
    if(dateVal==='Last 30 Days' && diffDays>30) return false;
    if(q && !(e.type.toLowerCase().includes(q) || e.city.toLowerCase().includes(q) || e.state.toLowerCase().includes(q))) return false;
    return true;
  });
  const tbody = document.getElementById('evTableBody');
  if(tbody) tbody.innerHTML = rows.length ? rows.map(evRowHTML).join('') :
    `<tr><td colspan="6" style="text-align:center;color:var(--ink-soft);padding:22px">No events match these filters.</td></tr>`;
}

RENDER.analytics = ()=>{
  return `
  <h1>Analytics</h1>
  <p style="color:var(--ink-soft);margin-top:-8px">Insights and trends for better preparedness.</p>
  <div class="stats">
    ${statCard('Total Events','1,248','12%')}
    ${statCard('Verified','842','16%')}
    ${statCard('Avg. Response','1.8 hrs','20%',true)}
    ${statCard('Disaster Events','93','4%')}
  </div>
  <div class="grid2">
    <div class="card"><h3>Event Trends (Last 7 Days)</h3>${svgLine(TREND,'var(--saffron-deep)')}</div>
    <div class="card"><h3>Event Distribution</h3>
      ${(()=>{ const typeDist=computeTypeDist(); const total=typeDist.reduce((a,b)=>a+b.n,0); return `
      <div style="display:flex;gap:18px;align-items:center">
        ${donut(typeDist)}
        <div style="font-size:12.5px">${typeDist.map(d=>`<div style="display:flex;gap:6px;align-items:center;margin-bottom:4px"><span style="width:9px;height:9px;border-radius:50%;background:${d.c};display:inline-block"></span>${d.t} — ${Math.round(d.n/total*100)}%</div>`).join('')}</div>
      </div>`; })()}
    </div>
  </div>
  <div class="grid2">
    <div class="card"><h3>Top Affected States</h3>
      ${(()=>{ const stateDist=computeStateDist(); const max=stateDist[0].n; return stateDist.map(s=>`<div class="loc-row"><span>${s.s}</span><span>${s.n} events</span></div>
      <div class="bar"><div style="width:${(s.n/max*100).toFixed(0)}%"></div></div>`).join(''); })()}
    </div>
    <div class="card"><h3>Recent Insights</h3>
      ${INSIGHTS.map(i=>`<div class="insight"><span class="ic">💡</span><span>${i}</span></div>`).join('')}
    </div>
  </div>
  <div class="grid2">
    <div class="card"><h3>Events by Type & Severity (Bar Chart)</h3>${stackedBarSVG()}</div>
    <div class="card"><h3>Events by State (Pie Chart)</h3>
      ${(()=>{ const st=computeStatePieDist(); const total=st.reduce((a,b)=>a+b.n,0); return `
      <div style="display:flex;gap:18px;align-items:center">
        ${pieSVG(st)}
        <div style="font-size:12.5px">${st.map(d=>`<div style="display:flex;gap:6px;align-items:center;margin-bottom:4px"><span style="width:9px;height:9px;border-radius:50%;background:${d.c};display:inline-block"></span>${d.t} — ${Math.round(d.n/total*100)}%</div>`).join('')}</div>
      </div>`; })()}
    </div>
  </div>
  <div class="grid2">
    <div class="card"><h3>Event Type Profile by Top States (Radar Chart)</h3>${radarStateEventSVG()}</div>
    <div class="card"><h3>State × Event Type (Heatmap)</h3>${heatmapStateEvent()}</div>
  </div>`;
};

RENDER.report = ()=>{
  return `
  <div class="hero" style="background:linear-gradient(120deg,var(--terracotta) 0%,var(--night) 140%)">
    <h1>Report a Disaster</h1><p>Help authorities respond faster. Share details with your state disaster management authority.</p>
  </div>
  <div class="steps">
    <div class="${reportStep===1?'active':''}">1. Incident Details</div>
    <div class="${reportStep===2?'active':''}">2. Attach Media</div>
    <div class="${reportStep===3?'active':''}">3. Review & Submit</div>
  </div>
  <div class="card" id="reportStepBox"></div>
  <div id="pipelineBox"></div>
  ${queue.length ? `<div class="card" style="margin-top:16px"><h3>Your Report Queue</h3>
    <table><thead><tr><th>ID</th><th>Type</th><th>Location</th><th>Files</th><th>Status</th></tr></thead>
    <tbody>${queue.map(q=>`<tr><td>${q.id}</td><td>${q.type}</td><td>${q.loc}</td><td>${q.files}</td>
      <td><span class="badge ${q.status==='Synced'?'synced':'waiting'}">${q.status}</span></td></tr>`).join('')}</tbody></table>
    <p style="font-size:12px;color:var(--ink-soft);margin-top:8px">Reports filed while offline sync automatically the moment your connection returns — no action needed.</p>
  </div>` : ''}
  `;
};

function reportStepHTML(){
  if(reportStep===1){
    return `
    <label>Disaster Type *</label>
    <select class="full" id="rt-type">
      <option value="">Select type</option>
      <option>Flood</option><option>Cyclone</option><option>Landslide</option><option>Heatwave</option><option>Storm</option><option>Other</option>
    </select>
    <label>Description *</label>
    <textarea id="rt-desc" placeholder="Describe the incident…"></textarea>
    <div class="row">
      <div><label>State *</label>
        <select class="full" id="rt-state"><option value="">Select state</option>${STATES.map(s=>`<option>${s}</option>`).join('')}</select></div>
      <div><label>District / City *</label>
        <input type="text" id="rt-district" placeholder="e.g. Chennai"></div>
    </div>
    <div style="margin-top:16px;display:flex;justify-content:flex-end"><button class="btn" onclick="reportNext()">Next: Attach Media →</button></div>`;
  }
  if(reportStep===2){
    return `
    <label>Attach Media</label>
    <div class="drop">📷 Click to upload images or videos<br><span style="font-size:11px">or drag and drop</span>
      <div><input type="file" id="rt-files" multiple accept="image/*,video/*" style="margin-top:10px"></div></div>
    <div class="thumbs" id="rt-thumbs"></div>
    <div style="margin-top:16px;display:flex;justify-content:space-between">
      <button class="btn secondary" onclick="reportStep=1;go('report')">← Back</button>
      <button class="btn" onclick="reportNext()">Next: Review & Submit →</button>
    </div>`;
  }
  const gps = "13.08°N, 77.20°W (simulated)";
  return `
    <h3 style="margin-top:0">Review your report</h3>
    <table>
      <tr><td style="color:var(--ink-soft)">Type</td><td>${reportDraft.type||'—'}</td></tr>
      <tr><td style="color:var(--ink-soft)">Description</td><td>${reportDraft.desc||'—'}</td></tr>
      <tr><td style="color:var(--ink-soft)">State / District</td><td>${reportDraft.state||'—'} / ${reportDraft.district||'—'}</td></tr>
      <tr><td style="color:var(--ink-soft)">Attachments</td><td>${reportDraft.files.length} file(s)</td></tr>
    </table>
    <div class="meta-box">
      <b>Location &amp; metadata (auto-extracted)</b><br>
      📍 GPS Location: ${gps}<br>
      🏙 City: ${reportDraft.district||'Auto'} &nbsp; 🗺 State: ${reportDraft.state||'Auto'}<br>
      🕒 Capture time: ${fmtISTNow()} &nbsp; 📎 Media metadata: extracted from attachments<br><br>
      This metadata will be shared with the State Disaster Management Authority.
    </div>
    <div style="margin-top:16px;display:flex;justify-content:space-between">
      <button class="btn secondary" onclick="reportStep=2;go('report')">← Back</button>
      <button class="btn" onclick="submitReport()">🚀 Submit to State Authority</button>
    </div>`;
}

function reportNext(){
  reportDraft.type = document.getElementById('rt-type') ? document.getElementById('rt-type').value : reportDraft.type;
  reportDraft.desc = document.getElementById('rt-desc') ? document.getElementById('rt-desc').value : reportDraft.desc;
  reportDraft.state = document.getElementById('rt-state') ? document.getElementById('rt-state').value : reportDraft.state;
  reportDraft.district = document.getElementById('rt-district') ? document.getElementById('rt-district').value : reportDraft.district;
  reportStep++;
  go('report');
}

function submitReport(){
  const id = 'DR-' + String(1000+queue.length).slice(-4);
  const rep = {id, type:reportDraft.type||'Other', loc:reportDraft.district||'Unknown', files:reportDraft.files.length||1,
    status: online ? 'Waiting' : 'Waiting', createdOffline: !online};
  queue.unshift(rep);
  saveQueue();
  document.getElementById('pipelineBox').innerHTML = `<div class="card">
    <h3>Submission trail — ${id}</h3>
    ${pipelineLog(id).map((s,i)=>`<div class="insight"><span class="ic">${i<3?'⏳':'✅'}</span><span>${s}</span></div>`).join('')}
    <p style="font-size:12px;color:var(--ink-soft);margin-top:10px">${online? 'Submitted — awaiting acknowledgement from the state authority.' : 'You are offline. This report is queued on your device (see Your Report Queue below) and will sync automatically the moment connectivity returns.'}</p>
  </div>`;
  toast(online ? `Report ${id} submitted` : `Report ${id} queued offline`);
  reportDraft = {type:'', desc:'', state:'', district:'', files:[]};
  reportStep = 1;
}

let settingsTab = 'notifications';
RENDER.settings = ()=>{
  const t = {notifications:'Notifications', language:'Language', about:'About'};
  let body = '';
  if(settingsTab==='notifications'){
    body = `<h3 style="margin-top:0">Notifications</h3>
    ${settingRow('Severe Weather Alerts','Warnings for extreme weather events','notif1')}
    ${settingRow('Disaster Alerts','Updates from disaster management authorities','notif2')}
    ${settingRow('Event Updates','Status changes and new events','notif3')}
    ${settingRow('System Notifications','App updates and maintenance','notif4', false)}`;
  } else if(settingsTab==='language'){
    body = `<h3 style="margin-top:0">Language</h3>
    <p style="font-size:13px;color:var(--ink-soft)">Choose your preferred language for the interface.</p>
    <select class="full" id="settingsLang" style="max-width:220px">${Object.entries(LANGS).map(([k,v])=>`<option value="${k}">${v}</option>`).join('')}</select>`;
  } else {
    body = `<h3 style="margin-top:0">About</h3>
    <p style="font-weight:700;margin-bottom:2px">☁ Weathered</p>
    <p style="font-size:13px;color:var(--ink-soft)">Real-time insights for a safer tomorrow.<br>Version 1.0.0 — account-free, offline-first.</p>`;
  }
  return `
  <h1>Settings</h1>
  <p style="color:var(--ink-soft);margin-top:-8px">Manage your notification and display preferences.</p>
  <div class="settings-grid">
    <div class="settings-nav">
      ${Object.entries(t).map(([k,v])=>`<button data-t="${k}" class="${settingsTab===k?'active':''}">${v}</button>`).join('')}
    </div>
    <div class="card">${body}</div>
  </div>`;
};
function settingRow(title,desc,key,def=true){
  const on = JSON.parse(localStorage.getItem(key) ?? def);
  return `<div class="setting-row"><div><div class="t">${title}</div><div class="d">${desc}</div></div>
    <button class="toggle ${on?'on':''}" data-key="${key}"></button></div>`;
}

/* ---------- Wire page-specific listeners ---------- */
function wirePage(id){
  if(id==='forecast' && online){
    const stateSel = document.getElementById('fcState');
    const citySel = document.getElementById('fcCity');
    const eventSel = document.getElementById('fcEvent');
    const dateSel = document.getElementById('fcDate');
    const populateCities = ()=>{
      const cities = REAL_LOCATIONS.filter(l=>l.state===stateSel.value);
      citySel.innerHTML = cities.map(l=>`<option value="${REAL_LOCATIONS.indexOf(l)}">${l.city}</option>`).join('');
    };
    const populateDates = (loc)=>{
      const fc = genForecast(loc);
      dateSel.innerHTML = `<option value="all">All 7 Days</option>` +
        fc.map((d,i)=>`<option value="${i}">${d.label}, ${d.date}</option>`).join('');
    };
    const renderBody = ()=>{
      const loc = REAL_LOCATIONS[+citySel.value];
      populateDates(loc);
      document.getElementById('fcBody').innerHTML = forecastBodyHTML(loc, eventSel.value, dateSel.value);
    };
    stateSel.value = userLoc.state;
    populateCities(); renderBody();
    stateSel.onchange = ()=>{ populateCities(); renderBody(); };
    citySel.onchange = renderBody;
    eventSel.onchange = ()=>{
      const loc = REAL_LOCATIONS[+citySel.value];
      document.getElementById('fcBody').innerHTML = forecastBodyHTML(loc, eventSel.value, dateSel.value);
    };
    dateSel.onchange = ()=>{
      const loc = REAL_LOCATIONS[+citySel.value];
      document.getElementById('fcBody').innerHTML = forecastBodyHTML(loc, eventSel.value, dateSel.value);
    };
  }
  if(id==='report'){ document.getElementById('reportStepBox').innerHTML = reportStepHTML();
    const fi = document.getElementById('rt-files');
    if(fi) fi.addEventListener('change', e=>{
      reportDraft.files = Array.from(e.target.files).map(f=>f.name);
      document.getElementById('rt-thumbs').innerHTML = reportDraft.files.map((f,i)=>
        `<div class="thumb">📎 ${f} <button onclick="removeFile(${i})">✕</button></div>`).join('');
    });
  }
  if(id==='events'){
    document.querySelectorAll('#evTabs button').forEach(b=> b.onclick = ()=>{
      document.querySelectorAll('#evTabs button').forEach(x=>x.classList.remove('active'));
      b.classList.add('active');
      evFilter = b.dataset.f;
      applyEventFilters();
    });
    ['evState','evSeverity','evDateRange'].forEach(elId=>{
      const el = document.getElementById(elId);
      if(el) el.onchange = applyEventFilters;
    });
    applyEventFilters();
  }
  if(id==='settings'){
    document.querySelectorAll('.settings-nav button').forEach(b=> b.onclick = ()=>{ settingsTab=b.dataset.t; go('settings'); });
    document.querySelectorAll('.toggle').forEach(b=> b.onclick = ()=>{
      const on = !b.classList.contains('on'); b.classList.toggle('on');
      localStorage.setItem(b.dataset.key, JSON.stringify(on));
    });
    const sl = document.getElementById('settingsLang');
    if(sl){ sl.value = lang; sl.onchange = e=> setLang(e.target.value); }
  }
}
function removeFile(i){ reportDraft.files.splice(i,1); go('report'); }

/* ---------- Language ---------- */
function setLang(l){
  lang = l; document.getElementById('langSel').value = l;
  document.querySelectorAll('[data-i18n="tagline"]').forEach(el=> el.textContent = I18N[l].tagline);
  buildNav();
  setActiveNav(currentPage);
  go(currentPage);
}

/* ---------- Network: real connectivity, auto-sync on reconnect ----------
   No manual UI toggle — this listens to the browser's real online/offline
   events only, still driving the offline banner, queued-report sync, and
   dimming the Forecast nav item when offline. */
function updateNetUI(){
  const fcBtn = document.querySelector('#nav button[data-id="forecast"]');
  if(fcBtn) fcBtn.style.opacity = online ? '1' : '0.45';
}
function handleOnline(){
  online = true; lastSyncTime = new Date(); updateNetUI();
  const pending = queue.filter(q=>q.status!=='Synced');
  if(pending.length){
    queue = queue.map(q=> ({...q, status:'Synced'}));
    saveQueue();
    toast(`${pending.length} offline report(s) auto-synced to the State Disaster Management Authority`);
  } else { toast('Back online — data synced'); }
  go(currentPage);
}
function handleOffline(){
  lastSyncTime = new Date(); // freeze: this is the last moment data was synced
  online = false; updateNetUI();
  toast('You are offline — data is frozen as of ' + fmtTime(lastSyncTime));
  go(currentPage);
}
window.addEventListener('online', handleOnline);
window.addEventListener('offline', handleOffline);
setInterval(()=>{ if(online) lastSyncTime = new Date(); }, 15000);

/* ---------- Dark / Light theme ---------- */
function setTheme(v){
  localStorage.setItem('wx_theme', v);
  if(v==='light') document.documentElement.setAttribute('data-theme','light');
  else if(v==='dark') document.documentElement.setAttribute('data-theme','dark');
  else document.documentElement.removeAttribute('data-theme');
}
document.getElementById('themeToggleBtn').onclick = ()=>{
  const cur = localStorage.getItem('wx_theme') || 'auto';
  const next = cur==='dark' ? 'light' : 'dark';
  setTheme(next);
  if(currentPage==='settings') go('settings');
};

/* ---------- PWA: register the service worker so the app shell (this page,
   its CSS/JS, icon, manifest) is cached and installable/offline-launchable.
   Only meaningful when served over http(s) from its own files — not inside
   the claude.ai artifact preview, which has no sw.js of its own. */
if('serviceWorker' in navigator){
  window.addEventListener('load', ()=>{
    navigator.serviceWorker.register('sw.js').catch(()=>{ /* no sw.js in this context — fine */ });
  });
}

/* ---------- Init ---------- */
setTheme(localStorage.getItem('wx_theme') || 'auto');
buildNav();
updateNetUI();
populateLangSelects();
initUserLocation();
document.getElementById('langSel').onchange = e=> setLang(e.target.value);
const searchEl = document.getElementById('searchBox');
searchEl.addEventListener('input', e=>{
  searchQuery = e.target.value;
  if(currentPage==='events') applyEventFilters();
});
searchEl.addEventListener('keydown', e=>{
  if(e.key==='Enter' && currentPage!=='events') go('events');
});
function populateLangSelects(){
  document.getElementById('langSel').innerHTML = Object.entries(LANGS).map(([k,v])=>`<option value="${k}">${v}</option>`).join('');
  document.getElementById('langSel').value = lang;
}
const start = (location.hash||'#home').slice(1);
go(RENDER[start] && start!=='offline' ? start : 'home');
