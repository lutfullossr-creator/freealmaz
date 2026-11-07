/* FreeAlmaz server — Node 18+, paketsiz (npm install kerak emas)
   Ishga tushirish:
     BOT_TOKEN=123:ABC BASE_URL=https://sizning-domen.uz CHANNEL=@kanal node server.js
   Bepul hostda (Render) baza yo'qolmasligi uchun Upstash Redis ulang: UPSTASH_URL va UPSTASH_TOKEN (kartasiz, bepul)
   Lokal sinash (Telegramsiz):  DEV=1 node server.js  ->  http://localhost:3000
   index.html shu papkada bo'lsin. Admin botga bir marta /start bosishi shart. */
const http=require("http"),fs=require("fs"),path=require("path"),crypto=require("crypto");
const E=process.env,TOKEN=E.BOT_TOKEN||"",ADMIN=+(E.ADMIN_ID||7692226666),PORT=+(E.PORT||3000);
const BASE=(E.BASE_URL||"").replace(/\/$/,""),CH=E.CHANNEL||"",DEV=E.DEV==="1";let BOT=E.BOT_USERNAME||"";
const TGB=E.TG_API||"https://api.telegram.org",UP=(E.UPSTASH_URL||"").replace(/\/$/,""),UT=E.UPSTASH_TOKEN||"",WH=crypto.createHash("sha256").update(TOKEN||"x").digest("hex").slice(0,24),DEVOK=DEV&&(!TOKEN||!!E.TG_API),WS=crypto.createHash("sha256").update(TOKEN+"/wh").digest("hex").slice(0,32);

const CFG={REF:22,MIN_REFS:15,MIN:[110,220,583],DAYS:[5,10,15,20,30,40,100],SPIN:[1,2,3,5,10,15,25,50],W:[30,25,20,10,7,5,2,1],SPIN_CD:6*3600e3,WELCOME:10,REF_TASKS:3,
  TASKS:[...(CH?[{id:"ch",t:"Kanalga a'zo bo'ling",r:10,url:"https://t.me/"+CH.replace("@",""),chat:CH}]:[]),
    {id:"sh",t:"Botni do'stga yuboring",r:5,share:1},
    {id:"r3",t:"3 ta do'st taklif qiling",r:15,ref:3},
    {id:"r10",t:"10 ta do'st taklif qiling",r:50,ref:10}]};

/* ===== BAZA: Upstash Redis (hostda) yoki db.json (lokal) ===== */
const DBF=path.join(__dirname,"db.json");let db={users:{},wd:{},n:0,tasks:[],tn:0,jr:{}},st,dT=false;const dU=new Set(),dW=new Set(),dJ=new Set();
const up=async(c,p="")=>{const r=await fetch(UP+p,{method:"POST",headers:{Authorization:"Bearer "+UT,"Content-Type":"application/json"},body:JSON.stringify(c)});if(!r.ok)throw new Error("Upstash "+r.status);return r.json()};
const hall=async k=>{const a=(await up(["HGETALL",k])).result||[],o={};for(let i=0;i<a.length;i+=2)o[a[i]]=JSON.parse(a[i+1]);return o};
async function load(){
  try{if(UP){db.users=await hall("fa:u");db.wd=await hall("fa:w");db.jr=await hall("fa:j");db.n=+((await up(["GET","fa:n"])).result||0);const tk=(await up(["GET","fa:tasks"])).result;if(tk){const o=JSON.parse(tk);db.tasks=o.list||[];db.tn=o.n||0;db.cfg=o.cfg||{};db.pause=o.pause||0;db.log=o.log||[]}}else db=JSON.parse(fs.readFileSync(DBF,"utf8"))}
  catch(e){if(UP){console.error("Baza yuklanmadi:",e.message);process.exit(1)}}
  db.users=db.users||{};db.wd=db.wd||{};db.n=db.n||0;db.tasks=db.tasks||[];db.tn=db.tn||0;db.jr=db.jr||{};db.cfg=db.cfg||{};db.pause=db.pause||0;db.log=db.log||[];Object.assign(CFG,db.cfg);
  for(const u of Object.values(db.users)){u.tasks=u.tasks||{};u.hist=u.hist||[]}}
async function persist(){
  if(!UP){try{fs.writeFile(DBF+".tmp",JSON.stringify(db),()=>fs.rename(DBF+".tmp",DBF,()=>{}))}catch(e){console.error("Saqlanmadi:",e.message)}return}
  const U=[...dU],W=[...dW],J=[...dJ],hT=dT;dU.clear();dW.clear();dJ.clear();dT=false;
  try{const c=[["SET","fa:n",String(db.n)]];if(hT)c.push(["SET","fa:tasks",JSON.stringify({n:db.tn,list:db.tasks,cfg:db.cfg,pause:db.pause,log:db.log})]);
    U.forEach(id=>db.users[id]&&c.push(["HSET","fa:u",String(id),JSON.stringify(db.users[id])]));
    W.forEach(id=>db.wd[id]&&c.push(["HSET","fa:w",id,JSON.stringify(db.wd[id])]));
    J.forEach(k=>c.push(["HSET","fa:j",k,"1"]));const rs=await up(c,"/pipeline"),bad=rs.find(x=>x.error);if(bad)throw new Error(bad.error)}
  catch(e){console.error("Saqlanmadi:",e.message);U.forEach(x=>dU.add(x));W.forEach(x=>dW.add(x));J.forEach(x=>dJ.add(x));if(hT)dT=true;setTimeout(save,5000)}}
const save=()=>{clearTimeout(st);st=setTimeout(persist,1000)};
const day=t=>Math.floor((t+5*36e5)/864e5); // UTC+5
const push=(u,t,txt,amt,x)=>{dU.add(u.id);u.hist.unshift({t,txt,amt,time:Date.now(),...x});u.hist.length=Math.min(u.hist.length,30)};
const pub=u=>({id:u.id,bal:u.bal,refs:u.refs,streak:u.streak,lastDay:u.lastDay,spinAt:u.spinAt,tasks:u.tasks,hist:u.hist,fl:(u.fl||[]).slice(-50).reverse().map(id=>{const f=db.users[id]||{};return{n:f.name||"",d:f.rdone?1:0,c:Math.min(f.tc||0,refNeed())}}),now:Date.now()});
const allTasks=()=>[...CFG.TASKS,...db.tasks];
const cnt=()=>allTasks().filter(k=>k.chat).length;
const refNeed=()=>Math.min(CFG.REF_TASKS,cnt());
const notify=(id,text)=>{if(TOKEN)tgApi("sendMessage",{chat_id:id,text})};
/* Do'st yetarli vazifa bajarsa, taklif qilgan odamga almaz beriladi */
function checkRef(u){if(!u.pend||u.rdone||(u.tc||0)<refNeed())return;u.rdone=1;dU.add(u.id);const R=db.users[u.by];if(!R)return;
  R.refs++;R.bal+=CFG.REF;push(R,"+","Do'st taklifi · "+(u.name||"do'st"),CFG.REF);
  notify(R.id,refNeed()>0?`Tabriklaymiz! Do'stingiz ${u.name||""} ${refNeed()} ta vazifani bajardi.\n+${CFG.REF} almaz qo'shildi.\nHisobingiz: ${R.bal} almaz`:`Tabriklaymiz! Do'stingiz ${u.name||""} referalingiz orqali kirdi.\n+${CFG.REF} almaz qo'shildi.\nHisobingiz: ${R.bal} almaz`)}
const pubCfg=()=>{const{W,TASKS:_,...r}=CFG;return{...r,REF_NEED:refNeed(),BOT,TASKS:allTasks().map(({chat,...k})=>({...k,cn:chat?1:0}))}};

/* ===== TELEGRAM ===== */
const tgApi=async(m,b)=>{try{const r=await fetch(`${TGB}/bot${TOKEN}/${m}`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)});return await r.json()}catch(e){return{ok:false}}};
const ready=(async()=>{await load();if(TOKEN){const me=await tgApi("getMe",{});if(me.ok)BOT=me.result.username}})();
function auth(req){
  const init=req.headers["x-init"];
  if(init&&TOKEN){const p=new URLSearchParams(init),h=p.get("hash");p.delete("hash");
    const str=[...p.entries()].sort(([a],[b])=>a<b?-1:1).map(([k,v])=>k+"="+v).join("\n");
    const key=crypto.createHmac("sha256","WebAppData").update(TOKEN).digest();
    {const x=Buffer.from(crypto.createHmac("sha256",key).update(str).digest("hex")),y=Buffer.from(String(h||""));if(x.length!==y.length||!crypto.timingSafeEqual(x,y))return null}
    if(Date.now()/1e3-(+p.get("auth_date")||0)>86400)return null;
    const u=JSON.parse(p.get("user")||"{}");if(!u.id)return null;
    return{id:u.id,name:u.first_name||u.username||"",sp:p.get("start_param")||""}}
  if(DEVOK&&req.headers["x-dev"])return{id:+req.headers["x-dev"],name:"Dev",sp:""};
  return null}
function user(a,ref){if(db.pause&&a.id!==ADMIN)throw"Texnik ishlar, keyinroq kiring";let u=db.users[a.id],fresh=false;
  if(!u){fresh=true;u=db.users[a.id]={id:a.id,bal:CFG.WELCOME,refs:0,streak:0,lastDay:0,spinAt:0,tasks:{},hist:[],ff:"",by:0,t:Date.now()};
    push(u,"+","Xush kelibsiz",CFG.WELCOME);
    const rid=+String(a.sp||ref||"").replace(/^ref_/,""),R=db.users[rid];
    if(rid&&rid!==a.id&&R){u.by=rid;u.pend=1;(R.fl=R.fl||[]).push(a.id);dU.add(rid);if(refNeed()>0)notify(rid,`Sizning do'stingiz ${a.name||""} referalingiz orqali kirdi.\nU ${refNeed()} ta vazifani bajarsa, sizga +${CFG.REF} almaz beriladi.`)}}
  if(u.ban&&a.id!==ADMIN)throw"Akkaunt bloklangan";
  u.name=a.name;if(fresh)checkRef(u);return[u,fresh]}

/* ===== API ===== */
const H={
  me:async(a,b)=>{const[u]=user(a,b.ref),fresh=!u.seen;u.seen=1;checkRef(u);dU.add(u.id);save();return{cfg:pubCfg(),s:pub(u),fresh,admin:a.id===ADMIN}},
  claim:async a=>{const[u]=user(a),d=day(Date.now());if(u.lastDay===d)throw"Bugun olingan";
    if(u.lastDay&&u.lastDay!==d-1)u.streak=0;const c=u.streak%7,v=CFG.DAYS[c];
    u.streak++;u.lastDay=d;u.bal+=v;push(u,"+","Kunlik bonus · "+(c+1)+"-kun",v);save();return{v,day:c+1,s:pub(u)}},
  spin:async a=>{const[u]=user(a);if(Date.now()<u.spinAt+CFG.SPIN_CD)throw"Hali erta";
    let r=crypto.randomInt(100),i=0;for(;i<7;i++){if((r-=CFG.W[i])<0)break}
    const v=CFG.SPIN[i];u.spinAt=Date.now();u.bal+=v;push(u,"+","G'ildirak",v);save();return{i,v,s:pub(u)}},
  task:async(a,b)=>{const[u]=user(a),k=allTasks().find(x=>x.id===b.id);if(!k)throw"Vazifa yo'q";if(u.tasks[k.id])throw"Bajarilgan";
    if(k.ref&&u.refs<k.ref)throw u.refs+" / "+k.ref+" do'st";
    u.tasks[k.id]=1;
    if(k.chat&&!db.jr[k.chat+":"+u.id]){const r=await tgApi("getChatMember",{chat_id:k.chat,user_id:u.id});
      if(!r.ok||!["member","administrator","creator"].includes(r.result.status)){delete u.tasks[k.id];dU.add(u.id);throw k.priv?"Avval kanalga qo'shilish so'rovini yuboring":"Avval kanalga a'zo bo'ling"}}
    u.bal+=k.r;push(u,"+",k.t,k.r);if(k.chat){u.tc=(u.tc||0)+1;checkRef(u)}save();return{v:k.r,s:pub(u)}},
  withdraw:async(a,b)=>{const[u]=user(a),m=+b.amount,ff=String(b.ff||"").trim();
    if(!/^\d{6,14}$/.test(ff))throw"Free Fire ID noto'g'ri";if(!CFG.MIN.includes(m))throw"Miqdor noto'g'ri";
    if(u.refs<CFG.MIN_REFS)throw"Yana "+(CFG.MIN_REFS-u.refs)+" do'st kerak";if(u.bal<m)throw"Almaz yetarli emas";
    if(Object.values(db.wd).some(w=>w.uid===u.id&&w.st==="wait"))throw"Oldingi so'rov kutilmoqda";
    const n=++db.n,id="w"+n;db.wd[id]={id,uid:u.id,ff,amt:m,st:"wait",time:Date.now()};
    u.bal-=m;u.ff=ff;push(u,"-","Yechish · ID "+ff,m,{id,st:"wait"});
    const sm=TOKEN?(await tgApi("sendMessage",{chat_id:ADMIN,text:`So'rov #${n}\n\nFoydalanuvchi: ${u.name||"-"} (${u.id})\nFree Fire ID: ${ff}\nMiqdor: ${m} almaz\nDo'stlar: ${u.refs}`,
      reply_markup:{inline_keyboard:[[{text:"Tasdiqlash",callback_data:"wd:ok:"+id},{text:"Rad etish",callback_data:"wd:no:"+id}]]}})):{ok:DEVOK},ok=sm.ok;
    if(!ok){delete db.wd[id];u.bal+=m;u.hist=u.hist.filter(h=>h.id!==id);dU.add(u.id);throw"Server xatosi, keyinroq urining"}
    if(sm.result)db.wd[id].mid=sm.result.message_id;dU.add(u.id);dW.add(id);save();return{s:pub(u)}},

  /* ===== ADMIN ===== */
  adm:async a=>{A(a);const us=Object.values(db.users),W=Object.values(db.wd),d1=Date.now()-864e5;
    return{tasks:db.tasks,cnt:cnt(),pause:db.pause,cfg:{REF:CFG.REF,MIN_REFS:CFG.MIN_REFS,WELCOME:CFG.WELCOME,MIN:CFG.MIN,DAYS:CFG.DAYS},top:[...us].sort((x,y)=>y.refs-x.refs).slice(0,5).map(u=>({id:u.id,n:u.name,refs:u.refs})),done:W.filter(w=>w.st!=="wait").sort((x,y)=>y.time-x.time).slice(0,8).map(w=>({st:w.st,amt:w.amt,ff:w.ff,n:(db.users[w.uid]||{}).name||""})),log:db.log.slice(0,12),users:us.length,fresh:us.filter(u=>u.t>d1).length,banned:us.filter(u=>u.ban).length,bal:us.reduce((x,u)=>x+u.bal,0),paid:W.filter(w=>w.st==="ok").reduce((x,w)=>x+w.amt,0),
      wd:W.filter(w=>w.st==="wait").map(w=>{const u=db.users[w.uid]||{};return{id:w.id,ff:w.ff,amt:w.amt,time:w.time,uid:w.uid,name:u.name||"",refs:u.refs||0}})}},
  adm_wd:async(a,b)=>{A(a);const w=resolveWd(b.id,!!b.ok);logA((b.ok?"Tasdiqlandi":"Rad etildi")+": "+w.amt+" almaz, ID "+w.ff);if(w.mid&&TOKEN)tgApi("editMessageReplyMarkup",{chat_id:ADMIN,message_id:w.mid,reply_markup:{inline_keyboard:[]}});return{ok:1}},
  adm_user:async(a,b)=>{A(a);const u=db.users[+b.id];if(!u)throw"Topilmadi";return{id:u.id,name:u.name,bal:u.bal,refs:u.refs,by:u.by,ban:!!u.ban,ff:u.ff,hist:u.hist.slice(0,6)}},
  adm_give:async(a,b)=>{A(a);const u=db.users[+b.id],n=Math.trunc(+b.amount);if(!u||!n)throw"Noto'g'ri";if(u.bal+n<0)throw"Balans manfiy bo'ladi";
    u.bal+=n;push(u,n>0?"+":"-","Admin",Math.abs(n));logA("Almaz "+(n>0?"+":"")+n+": ID "+u.id);save();return{bal:u.bal}},
  adm_ban:async(a,b)=>{A(a);const u=db.users[+b.id];if(!u)throw"Topilmadi";u.ban=b.on?1:0;dU.add(u.id);logA((u.ban?"Bloklandi":"Blokdan chiqarildi")+": ID "+u.id);save();return{ban:!!u.ban}},
  adm_cast:async(a,b)=>{A(a);const text=String(b.text||"").trim().slice(0,1000);if(!text)throw"Matn yo'q";const ids=Object.keys(db.users);logA("Xabar: "+ids.length+" kishiga");
    (async()=>{for(const id of ids){await tgApi("sendMessage",{chat_id:id,text});await new Promise(z=>setTimeout(z,50))}})();return{n:ids.length}},
  adm_cfg:async(a,b)=>{A(a);const n=(v,lo,hi)=>{v=Math.trunc(+v);if(!(v>=lo&&v<=hi))throw"Qiymat noto'g'ri";return v};
    const arr=(x,len)=>{const l=String(x).split(",").map(v=>Math.trunc(+v.trim()));if(l.some(v=>!(v>0&&v<=100000))||(len?l.length!==len:(l.length<1||l.length>3)))throw len?"Kunlik bonus 7 ta son bo'lsin":"Yechish miqdori 1-3 ta son bo'lsin";return l};
    const o={REF:n(b.REF,1,1000),MIN_REFS:n(b.MIN_REFS,0,1000),WELCOME:n(b.WELCOME,0,1000),MIN:arr(b.MIN,0),DAYS:arr(b.DAYS,7)};
    Object.assign(CFG,o);db.cfg=o;logA("Sozlamalar o'zgardi");save();return{ok:1}},
  adm_pause:async(a,b)=>{A(a);db.pause=b.on?1:0;logA("Texnik ishlar: "+(db.pause?"yoqildi":"o'chirildi"));save();return{pause:db.pause}},
  adm_task_add:async(a,b)=>{A(a);const t=String(b.t||"").trim().slice(0,60),r=Math.trunc(+b.r),v=String(b.v||"").trim(),kind=b.k;
    if(!t||!(r>0&&r<=1000))throw"Nom va mukofot (1-1000) kerak";const k={id:"t"+(++db.tn),t,r};
    if(kind==="ref"){const n=Math.trunc(+v);if(!(n>0))throw"Do'stlar sonini kiriting";k.ref=Math.min(n,1000)}
    else if(kind==="pub"){const m=v.match(/^https:\/\/t\.me\/([A-Za-z0-9_]{5,})\/?$/);if(!m)throw"Havola: https://t.me/kanal";k.url=v;k.chat="@"+m[1]}
    else if(kind==="priv"){if(!/^-100\d{5,}$/.test(v))throw"Kanal ID -100... ko'rinishida bo'lsin";
      const l=await tgApi("createChatInviteLink",{chat_id:v,creates_join_request:true,name:"FreeAlmaz"});if(!l.ok)throw"Bot kanalda admin emas (havola yaratilmadi)";
      k.url=l.result.invite_link;k.chat=v;k.priv=1}
    else{if(!/^https:\/\/\S+$/.test(v))throw"Havola https:// bilan boshlansin";k.url=v}
    db.tasks.push(k);dT=true;logA("Vazifa qo'shildi: "+t);save();return{ok:1}},
  adm_task_del:async(a,b)=>{A(a);db.tasks=db.tasks.filter(x=>x.id!==b.id);dT=true;logA("Vazifa o'chirildi");save();return{ok:1}}
};
const A=a=>{if(a.id!==ADMIN)throw"Ruxsat yo'q"};
const logA=x=>{db.log.unshift({t:Date.now(),x});db.log.length=Math.min(db.log.length,40);dT=true};
function resolveWd(id,ok){const w=db.wd[id];if(!w||w.st!=="wait")throw"Allaqachon hal qilingan";
  const u=db.users[w.uid];w.st=ok?"ok":"no";const h=u.hist.find(h=>h.id===id);if(h)h.st=w.st;dU.add(u.id);dW.add(id);
  if(!ok){u.bal+=w.amt;push(u,"+","Qaytarildi",w.amt)}save();
  tgApi("sendMessage",{chat_id:u.id,text:ok?`So'rovingiz tasdiqlandi: ${w.amt} almaz (ID ${w.ff}).`:`So'rovingiz rad etildi, ${w.amt} almaz qaytarildi.`});return w}

const RL=new Map();const limited=(k,max,ms)=>{const n=Date.now(),e=RL.get(k);if(!e||n>e.t){RL.set(k,{t:n+ms,c:1});return false}return ++e.c>max};
setInterval(()=>{const n=Date.now();for(const[k,e]of RL)if(n>e.t)RL.delete(k)},60000).unref();
const CSP="default-src 'self'; script-src 'self' 'unsafe-inline' https://telegram.org; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self'; frame-ancestors https://web.telegram.org https://*.telegram.org https://telegram.org; base-uri 'none'; form-action 'none'";
http.createServer(async(req,res)=>{
  const url=new URL(req.url,"http://x"),send=(c,o)=>{res.writeHead(c,{"Content-Type":"application/json"});res.end(JSON.stringify(o))};
  res.setHeader("X-Content-Type-Options","nosniff");res.setHeader("Referrer-Policy","no-referrer");res.setHeader("Cache-Control","no-store");
  const ip=String(req.headers["x-forwarded-for"]||req.socket.remoteAddress||"").split(",")[0].trim();
  if(url.pathname==="/health"){res.writeHead(200);return res.end("ok")}
  await ready;
  if(req.method==="POST"&&url.pathname==="/tg/"+WH){if(TOKEN&&req.headers["x-telegram-bot-api-secret-token"]!==WS){res.writeHead(403);return res.end()}let b="";for await(const c of req)b+=c;res.writeHead(200);res.end();try{await upd(JSON.parse(b))}catch(e){console.error(e)}return}
  if(req.method==="GET"&&(url.pathname==="/"||url.pathname==="/index.html")){res.writeHead(200,{"Content-Type":"text/html; charset=utf-8","Content-Security-Policy":CSP});return fs.createReadStream(path.join(__dirname,"index.html")).pipe(res)}
  if(req.method==="POST"&&url.pathname.startsWith("/api/")){
    if(limited("ip:"+ip,300,60000))return send(429,{e:"Juda ko'p so'rov"});
    const a=auth(req);if(!a)return send(limited("bad:"+ip,30,60000)?429:401,{e:"Telegram orqali oching"});
    if(limited("u:"+a.id,40,10000))return send(429,{e:"Sekinroq, juda ko'p so'rov"});
    let b="";for await(const c of req){b+=c;if(b.length>1e4)return send(413,{e:"Katta so'rov"})}
    try{b=JSON.parse(b||"{}")}catch(e){b={}}
    const h=H[url.pathname.slice(5)];if(!h)return send(404,{e:"Topilmadi"});
    try{send(200,await h(a,b))}catch(e){typeof e==="string"?send(400,{e}):(console.error(e),send(500,{e:"Server xatosi"}))}
  }else{res.writeHead(404);res.end()}
}).listen(PORT,()=>console.log(`http://localhost:${PORT}  admin=${ADMIN}${TOKEN?"":"  (BOT_TOKEN yo'q)"}`));

/* ===== BOT: /start, admin tugmalari ===== */
async function upd(x){
  const jq=x.chat_join_request;if(jq){const key=jq.chat.id+":"+jq.from.id;db.jr[key]=1;dJ.add(key);save();return}
  const mc=x.my_chat_member;if(mc){if(mc.chat.type!=="private"&&mc.new_chat_member.status==="administrator")notify(ADMIN,`Bot kanalga admin bo'ldi: ${mc.chat.title||""}\nID: ${mc.chat.id}${mc.chat.username?"\n@"+mc.chat.username:""}`);return}
  const q=x.callback_query;
  if(q){const[,act,id]=(q.data||"").split(":"),ans=text=>tgApi("answerCallbackQuery",{callback_query_id:q.id,text});
    if(q.from.id!==ADMIN)return ans("Ruxsat yo'q");
    try{resolveWd(id,act==="ok")}catch(e){return ans(String(e))}
    await tgApi("editMessageText",{chat_id:q.message.chat.id,message_id:q.message.message_id,text:q.message.text+"\n\nHolat: "+(act==="ok"?"TASDIQLANDI":"RAD ETILDI")});
    return ans(act==="ok"?"Tasdiqlandi":"Rad etildi")}
  const m=x.message;if(!m||!m.text)return;
  if(m.text.startsWith("/start")){const p=m.text.split(" ")[1]||"",url=BASE+(p.startsWith("ref_")?"?ref="+encodeURIComponent(p.slice(4)):"");
    try{user({id:m.from.id,name:m.from.first_name||m.from.username||"",sp:p});save()}catch(e){return}
    return tgApi("sendMessage",{chat_id:m.chat.id,text:"Free Almaz: bonus, g'ildirak, vazifalar va do'stlar uchun almaz yig'ing.",...(BASE?{reply_markup:{inline_keyboard:[[{text:"Ochish",web_app:{url}}]]}}:{})})}
  if(m.text==="/stat"&&m.from.id===ADMIN){const pend=Object.values(db.wd).filter(w=>w.st==="wait").length;
    return tgApi("sendMessage",{chat_id:m.chat.id,text:`Foydalanuvchilar: ${Object.keys(db.users).length}\nKutilayotgan so'rovlar: ${pend}`})}
}
(async()=>{await ready;if(!TOKEN)return;
  if(BASE)tgApi("setChatMenuButton",{menu_button:{type:"web_app",text:"Ochish",web_app:{url:BASE}}});
  if(BASE&&E.POLL!=="1"){await tgApi("setWebhook",{url:BASE+"/tg/"+WH,secret_token:WS,allowed_updates:["message","callback_query","chat_join_request","my_chat_member"]});console.log("webhook: yoqildi");return} // hostda: bot uxlamaydi
  await tgApi("deleteWebhook",{});
  for(let off=0;;){const r=await tgApi("getUpdates",{offset:off,timeout:25,allowed_updates:["message","callback_query","chat_join_request","my_chat_member"]});
    if(!r.ok){await new Promise(z=>setTimeout(z,3000));continue}
    for(const x of r.result){off=x.update_id+1;try{await upd(x)}catch(e){console.error(e)}}}})();
