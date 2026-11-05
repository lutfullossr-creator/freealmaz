/* FreeAlmaz server — Node 18+, paketsiz (npm install kerak emas)
   Ishga tushirish:
     BOT_TOKEN=123:ABC BASE_URL=https://sizning-domen.uz CHANNEL=@kanal node server.js
   Bepul hostda (Render) baza yo'qolmasligi uchun Upstash Redis ulang: UPSTASH_URL va UPSTASH_TOKEN (kartasiz, bepul)
   Lokal sinash (Telegramsiz):  DEV=1 node server.js  ->  http://localhost:3000
   index.html shu papkada bo'lsin. Admin botga bir marta /start bosishi shart. */
const http=require("http"),fs=require("fs"),path=require("path"),crypto=require("crypto");
const E=process.env,TOKEN=E.BOT_TOKEN||"",ADMIN=+(E.ADMIN_ID||7692226666),PORT=+(E.PORT||3000);
const BASE=(E.BASE_URL||"").replace(/\/$/,""),CH=E.CHANNEL||"",DEV=E.DEV==="1";let BOT=E.BOT_USERNAME||"";
const UP=(E.UPSTASH_URL||"").replace(/\/$/,""),UT=E.UPSTASH_TOKEN||"",WH=crypto.createHash("sha256").update(TOKEN||"x").digest("hex").slice(0,24);

const CFG={REF:22,MIN_REFS:15,MIN:[110,220,583],DAYS:[5,10,15,20,30,40,100],SPIN:[1,2,3,5,10,15,25,50],W:[30,25,20,10,7,5,2,1],SPIN_CD:6*3600e3,WELCOME:10,
  TASKS:[...(CH?[{id:"ch",t:"Kanalga a'zo bo'ling",r:10,url:"https://t.me/"+CH.replace("@",""),chat:CH}]:[]),
    {id:"sh",t:"Botni do'stga yuboring",r:5,share:1},
    {id:"r3",t:"3 ta do'st taklif qiling",r:15,ref:3},
    {id:"r10",t:"10 ta do'st taklif qiling",r:50,ref:10}]};

/* ===== BAZA: Upstash Redis (hostda) yoki db.json (lokal) ===== */
const DBF=path.join(__dirname,"db.json");let db={users:{},wd:{},n:0},st;const dU=new Set(),dW=new Set();
const up=async(c,p="")=>{const r=await fetch(UP+p,{method:"POST",headers:{Authorization:"Bearer "+UT,"Content-Type":"application/json"},body:JSON.stringify(c)});if(!r.ok)throw new Error("Upstash "+r.status);return r.json()};
const hall=async k=>{const a=(await up(["HGETALL",k])).result||[],o={};for(let i=0;i<a.length;i+=2)o[a[i]]=JSON.parse(a[i+1]);return o};
async function load(){
  try{if(UP){db.users=await hall("fa:u");db.wd=await hall("fa:w");db.n=+((await up(["GET","fa:n"])).result||0)}else db=JSON.parse(fs.readFileSync(DBF,"utf8"))}
  catch(e){if(UP){console.error("Baza yuklanmadi:",e.message);process.exit(1)}}
  db.users=db.users||{};db.wd=db.wd||{};db.n=db.n||0;
  for(const u of Object.values(db.users)){u.tasks=u.tasks||{};u.hist=u.hist||[]}}
async function persist(){
  if(!UP){try{fs.writeFile(DBF+".tmp",JSON.stringify(db),()=>fs.rename(DBF+".tmp",DBF,()=>{}))}catch(e){console.error("Saqlanmadi:",e.message)}return}
  const U=[...dU],W=[...dW];dU.clear();dW.clear();
  try{const c=[["SET","fa:n",String(db.n)]];
    U.forEach(id=>db.users[id]&&c.push(["HSET","fa:u",String(id),JSON.stringify(db.users[id])]));
    W.forEach(id=>db.wd[id]&&c.push(["HSET","fa:w",id,JSON.stringify(db.wd[id])]));
    const rs=await up(c,"/pipeline"),bad=rs.find(x=>x.error);if(bad)throw new Error(bad.error)}
  catch(e){console.error("Saqlanmadi:",e.message);U.forEach(x=>dU.add(x));W.forEach(x=>dW.add(x));setTimeout(save,5000)}}
const save=()=>{clearTimeout(st);st=setTimeout(persist,1000)};
const day=t=>Math.floor((t+5*36e5)/864e5); // UTC+5
const push=(u,t,txt,amt,x)=>{dU.add(u.id);u.hist.unshift({t,txt,amt,time:Date.now(),...x});u.hist.length=Math.min(u.hist.length,30)};
const pub=u=>({id:u.id,bal:u.bal,refs:u.refs,streak:u.streak,lastDay:u.lastDay,spinAt:u.spinAt,tasks:u.tasks,hist:u.hist,now:Date.now()});
const pubCfg=()=>{const{W,TASKS,...r}=CFG;return{...r,BOT,TASKS:TASKS.map(({chat,...k})=>k)}};

/* ===== TELEGRAM ===== */
const tgApi=async(m,b)=>{try{const r=await fetch(`https://api.telegram.org/bot${TOKEN}/${m}`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)});return await r.json()}catch(e){return{ok:false}}};
const ready=(async()=>{await load();if(TOKEN){const me=await tgApi("getMe",{});if(me.ok)BOT=me.result.username}})();
function auth(req){
  const init=req.headers["x-init"];
  if(init&&TOKEN){const p=new URLSearchParams(init),h=p.get("hash");p.delete("hash");
    const str=[...p.entries()].sort(([a],[b])=>a<b?-1:1).map(([k,v])=>k+"="+v).join("\n");
    const key=crypto.createHmac("sha256","WebAppData").update(TOKEN).digest();
    if(crypto.createHmac("sha256",key).update(str).digest("hex")!==h)return null;
    if(Date.now()/1e3-(+p.get("auth_date")||0)>7*86400)return null;
    const u=JSON.parse(p.get("user")||"{}");if(!u.id)return null;
    return{id:u.id,name:u.first_name||u.username||"",sp:p.get("start_param")||""}}
  if(DEV&&req.headers["x-dev"])return{id:+req.headers["x-dev"],name:"Dev",sp:""};
  return null}
function user(a,ref){let u=db.users[a.id],fresh=false;
  if(!u){fresh=true;u=db.users[a.id]={id:a.id,bal:CFG.WELCOME,refs:0,streak:0,lastDay:0,spinAt:0,tasks:{},hist:[],ff:"",by:0};
    push(u,"+","Xush kelibsiz",CFG.WELCOME);
    const rid=+String(a.sp||ref||"").replace(/^ref_/,""),R=db.users[rid];
    if(rid&&rid!==a.id&&R){u.by=rid;R.refs++;R.bal+=CFG.REF;push(R,"+","Do'st taklifi",CFG.REF)}}
  u.name=a.name;return[u,fresh]}

/* ===== API ===== */
const H={
  me:async(a,b)=>{const[u,fresh]=user(a,b.ref);save();return{cfg:pubCfg(),s:pub(u),fresh}},
  claim:async a=>{const[u]=user(a),d=day(Date.now());if(u.lastDay===d)throw"Bugun olingan";
    if(u.lastDay&&u.lastDay!==d-1)u.streak=0;const c=u.streak%7,v=CFG.DAYS[c];
    u.streak++;u.lastDay=d;u.bal+=v;push(u,"+","Kunlik bonus · "+(c+1)+"-kun",v);save();return{v,day:c+1,s:pub(u)}},
  spin:async a=>{const[u]=user(a);if(Date.now()<u.spinAt+CFG.SPIN_CD)throw"Hali erta";
    let r=crypto.randomInt(100),i=0;for(;i<7;i++){if((r-=CFG.W[i])<0)break}
    const v=CFG.SPIN[i];u.spinAt=Date.now();u.bal+=v;push(u,"+","G'ildirak",v);save();return{i,v,s:pub(u)}},
  task:async(a,b)=>{const[u]=user(a),k=CFG.TASKS.find(x=>x.id===b.id);if(!k)throw"Vazifa yo'q";if(u.tasks[k.id])throw"Bajarilgan";
    if(k.ref&&u.refs<k.ref)throw u.refs+" / "+k.ref+" do'st";
    u.tasks[k.id]=1;
    if(k.chat){const r=await tgApi("getChatMember",{chat_id:k.chat,user_id:u.id});
      if(!r.ok||!["member","administrator","creator"].includes(r.result.status)){delete u.tasks[k.id];dU.add(u.id);throw"Avval kanalga a'zo bo'ling"}}
    u.bal+=k.r;push(u,"+",k.t,k.r);save();return{v:k.r,s:pub(u)}},
  withdraw:async(a,b)=>{const[u]=user(a),m=+b.amount,ff=String(b.ff||"").trim();
    if(!/^\d{6,14}$/.test(ff))throw"Free Fire ID noto'g'ri";if(!CFG.MIN.includes(m))throw"Miqdor noto'g'ri";
    if(u.refs<CFG.MIN_REFS)throw"Yana "+(CFG.MIN_REFS-u.refs)+" do'st kerak";if(u.bal<m)throw"Almaz yetarli emas";
    if(Object.values(db.wd).some(w=>w.uid===u.id&&w.st==="wait"))throw"Oldingi so'rov kutilmoqda";
    const n=++db.n,id="w"+n;db.wd[id]={id,uid:u.id,ff,amt:m,st:"wait",time:Date.now()};
    u.bal-=m;u.ff=ff;push(u,"-","Yechish · ID "+ff,m,{id,st:"wait"});
    const ok=TOKEN?(await tgApi("sendMessage",{chat_id:ADMIN,text:`So'rov #${n}\n\nFoydalanuvchi: ${u.name||"-"} (${u.id})\nFree Fire ID: ${ff}\nMiqdor: ${m} almaz\nDo'stlar: ${u.refs}`,
      reply_markup:{inline_keyboard:[[{text:"Tasdiqlash",callback_data:"wd:ok:"+id},{text:"Rad etish",callback_data:"wd:no:"+id}]]}})).ok:DEV;
    if(!ok){delete db.wd[id];u.bal+=m;u.hist=u.hist.filter(h=>h.id!==id);dU.add(u.id);throw"Server xatosi, keyinroq urining"}
    dU.add(u.id);dW.add(id);save();return{s:pub(u)}}
};

http.createServer(async(req,res)=>{
  const url=new URL(req.url,"http://x"),send=(c,o)=>{res.writeHead(c,{"Content-Type":"application/json"});res.end(JSON.stringify(o))};
  if(url.pathname==="/health"){res.writeHead(200);return res.end("ok")}
  await ready;
  if(req.method==="POST"&&url.pathname==="/tg/"+WH){let b="";for await(const c of req)b+=c;res.writeHead(200);res.end();try{await upd(JSON.parse(b))}catch(e){console.error(e)}return}
  if(req.method==="GET"&&(url.pathname==="/"||url.pathname==="/index.html")){res.writeHead(200,{"Content-Type":"text/html; charset=utf-8"});return fs.createReadStream(path.join(__dirname,"index.html")).pipe(res)}
  if(req.method==="POST"&&url.pathname.startsWith("/api/")){
    const a=auth(req);if(!a)return send(401,{e:"Telegram orqali oching"});
    let b="";for await(const c of req){b+=c;if(b.length>1e4)return send(413,{e:"Katta so'rov"})}
    try{b=JSON.parse(b||"{}")}catch(e){b={}}
    const h=H[url.pathname.slice(5)];if(!h)return send(404,{e:"Topilmadi"});
    try{send(200,await h(a,b))}catch(e){typeof e==="string"?send(400,{e}):(console.error(e),send(500,{e:"Server xatosi"}))}
  }else{res.writeHead(404);res.end()}
}).listen(PORT,()=>console.log(`http://localhost:${PORT}  admin=${ADMIN}${TOKEN?"":"  (BOT_TOKEN yo'q)"}`));

/* ===== BOT: /start, admin tugmalari ===== */
async function upd(x){
  const q=x.callback_query;
  if(q){const[,act,id]=(q.data||"").split(":"),ans=text=>tgApi("answerCallbackQuery",{callback_query_id:q.id,text});
    if(q.from.id!==ADMIN)return ans("Ruxsat yo'q");
    const w=db.wd[id];if(!w||w.st!=="wait")return ans("Allaqachon hal qilingan");
    const ok=act==="ok",u=db.users[w.uid];w.st=ok?"ok":"no";
    const h=u.hist.find(h=>h.id===id);if(h)h.st=w.st;dU.add(u.id);dW.add(id);
    if(!ok){u.bal+=w.amt;push(u,"+","Qaytarildi",w.amt)}save();
    await tgApi("editMessageText",{chat_id:q.message.chat.id,message_id:q.message.message_id,text:q.message.text+"\n\nHolat: "+(ok?"TASDIQLANDI":"RAD ETILDI")});
    await ans(ok?"Tasdiqlandi":"Rad etildi");
    return tgApi("sendMessage",{chat_id:u.id,text:ok?`So'rovingiz tasdiqlandi: ${w.amt} almaz (ID ${w.ff}).`:`So'rovingiz rad etildi, ${w.amt} almaz qaytarildi.`})}
  const m=x.message;if(!m||!m.text)return;
  if(m.text.startsWith("/start")){const p=m.text.split(" ")[1]||"",url=BASE+(p.startsWith("ref_")?"?ref="+encodeURIComponent(p.slice(4)):"");
    return tgApi("sendMessage",{chat_id:m.chat.id,text:"Free Almaz",...(BASE?{reply_markup:{inline_keyboard:[[{text:"Ochish",web_app:{url}}]]}}:{})})}
  if(m.text==="/stat"&&m.from.id===ADMIN){const pend=Object.values(db.wd).filter(w=>w.st==="wait").length;
    return tgApi("sendMessage",{chat_id:m.chat.id,text:`Foydalanuvchilar: ${Object.keys(db.users).length}\nKutilayotgan so'rovlar: ${pend}`})}
}
(async()=>{await ready;if(!TOKEN)return;
  if(BASE)tgApi("setChatMenuButton",{menu_button:{type:"web_app",text:"Ochish",web_app:{url:BASE}}});
  if(BASE&&E.POLL!=="1"){await tgApi("setWebhook",{url:BASE+"/tg/"+WH,allowed_updates:["message","callback_query"]});console.log("webhook: yoqildi");return} // hostda: bot uxlamaydi
  await tgApi("deleteWebhook",{});
  for(let off=0;;){const r=await tgApi("getUpdates",{offset:off,timeout:25,allowed_updates:["message","callback_query"]});
    if(!r.ok){await new Promise(z=>setTimeout(z,3000));continue}
    for(const x of r.result){off=x.update_id+1;try{await upd(x)}catch(e){console.error(e)}}}})();
