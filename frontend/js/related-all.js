const token = localStorage.getItem("access_token");
if (!token) window.location.href = "/static/login.html";

let cu=null, view="key", tab="all", page=1, selUp="", selUsr="", selCat="";
const PS=20, FS=15;

async function af(url,o={}){
    const r=await fetch(url,{...o,headers:{...(o.headers||{}),"Authorization":"Bearer "+token}});
    if(r.status===401){localStorage.removeItem("access_token");window.location.href="/static/login.html";}
    return r;
}
function toast(m,t="success"){
    const c=document.getElementById("tc"),el=document.createElement("div");
    el.className=`toast ${t}`;el.textContent=m;c.appendChild(el);
    setTimeout(()=>{el.style.opacity="0";setTimeout(()=>el.remove(),300);},3500);
}
function fmt(n){if(n==null)return"—";if(n>=1e6)return(n/1e6).toFixed(1)+"M";if(n>=1e3)return(n/1e3).toFixed(1)+"k";return n.toLocaleString();}

async function init(){
    const r=await af("/me"); cu=await r.json();
    if(cu.role==="admin"){
        document.getElementById("adminWrap").style.display="flex";
        await loadUsers();
    } else {
        document.getElementById("categoryWrap").style.display="flex";
        await loadCategories();
    }
    await loadFiles();
    await Promise.all([loadStats(),load()]);
}

async function loadCategories(){
    const r=await af("/categories");if(!r.ok)return;
    const cats=await r.json();const s=document.getElementById("fCat");
    s.innerHTML='<option value="">All Categories</option>';
    cats.forEach(c=>{const o=document.createElement("option");o.value=c.id;o.textContent=c.name.length>45?c.name.slice(0,43)+"\u2026":c.name;s.appendChild(o);});
}

async function loadUsers(){
    const r=await af("/admin/users-with-stats");if(!r.ok)return;
    const us=await r.json();const s=document.getElementById("fUsr");
    us.filter(u=>u.role!=="admin").forEach(u=>{const o=document.createElement("option");o.value=u.id;o.textContent=u.email.length>40?u.email.slice(0,38)+"…":u.email;s.appendChild(o);});
}

async function loadFiles(){
    let url="/uploads";
    const usrSel=document.getElementById("fUsr");
    const usrVal=usrSel?.value||"";
    if(usrVal)url+="?created_by_user_id="+usrVal;
    const r=await af(url);if(!r.ok)return;
    const us=await r.json();const s=document.getElementById("fUp");
    s.innerHTML='<option value="">All Files</option>';
    us.filter(u=>u.processing_status==="ready").forEach(u=>{const o=document.createElement("option");o.value=u.upload_id;o.textContent=u.filename.length>55?u.filename.slice(0,53)+"\u2026":u.filename;s.appendChild(o);});
}

async function loadStats(){
    const p=bp(true);const r=await af("/related-all-stats?"+p);if(!r.ok)return;
    const d=await r.json();
    document.getElementById("sTF").textContent=fmt(d.total_files);
    document.getElementById("sEG").textContent=fmt(d.email_groups);
    document.getElementById("sPG").textContent=fmt(d.phone_groups);
    document.getElementById("sBG").textContent=fmt(d.both_groups);
    document.getElementById("tAll").textContent=fmt((d.email_groups||0)+(d.phone_groups||0)+(d.both_groups||0));
    document.getElementById("tEmail").textContent=fmt(d.email_groups);
    document.getElementById("tPhone").textContent=fmt(d.phone_groups);
    document.getElementById("tMerged").textContent=fmt(d.both_groups);
}

function bp(statsOnly=false){
    const p=new URLSearchParams();
    if(selUp) p.append("upload_id",selUp);
    if(selUsr)p.append("user_id",selUsr);
    if(selCat)p.append("category_id",selCat);
    if(!statsOnly){
        p.append("match_type",tab);
        p.append("sort",document.getElementById("sortSel").value);
        p.append("page",page);
        p.append("page_size",view==="file"?FS:PS);
    }
    return p.toString();
}

async function load(){
    const area=document.getElementById("content");
    area.innerHTML=`<div class="ldg"><div class="spin"></div><p style="color:#64748b;font-size:14px;">Loading…</p></div>`;
    document.getElementById("pag").innerHTML="";
    view==="key"?await loadKeyView(area):await loadFileView(area);
}

/* ── BY MATCH KEY ── */
async function loadKeyView(area){
    const r=await af("/related-grouped-all?"+bp());
    if(!r.ok){toast("Failed to load","error");area.innerHTML="";return;}
    const d=await r.json();
    const tot=d.total_groups, tp=Math.ceil(tot/PS), st=(page-1)*PS;
    setRI(tot,st,Math.min(st+PS,tot),"group");
    if(!d.groups.length){area.innerHTML=emptyHTML("No related records found","No duplicate contacts detected with the current filters.");return;}
    const list=document.createElement("div");list.className="glist";
    d.groups.forEach(g=>list.appendChild(mkCard(g)));
    area.innerHTML="";area.appendChild(list);
    renderPag(tp);
}

function mkCard(g){
    const card=document.createElement("div");card.className="gcard";
    const bc=g.match_type==="email"?"mbe":g.match_type==="phone"?"mbp":"mbm";
    const bl=g.match_type==="email"?"EMAIL":g.match_type==="phone"?"PHONE":"EMAIL+PHONE";
    const cross=g.file_count>1;
    const allCols=new Set();(g.records||[]).forEach(r=>Object.keys(r.data||{}).forEach(k=>allCols.add(k)));
    const cols=[...allCols].filter(c=>!c.startsWith("original_")&&!c.startsWith("raw_")).slice(0,8);
    const ftags=(g.filenames||[]).map(f=>`<span class="ftag">📄 ${f.length>44?f.slice(0,42)+"…":f}</span>`).join("");
    const recs=g.records||[], ff=recs[0]?.filename||"";
    const PREV=5;
    const adminTH=cu.role==="admin"?"<th>Uploader</th>":"";
    const rowsHTML=recs.slice(0,PREV).map(r=>mkRow(r,cols,ff)).join("");
    const totalCount=g.record_count;
    const shownCount=Math.min(PREV,recs.length);
    const remaining=totalCount-shownCount;
    const smr=remaining>0?`<tr class="smr-row"><td colspan="${cols.length+1+(cu.role==="admin"?1:0)}"><button class="smr-btn" onclick="expandRows(this)">Show ${Math.min(remaining,100)} more of ${remaining} remaining ▾</button></td></tr>`:"";
    card.innerHTML=`
      <div class="ghdr" onclick="this.parentElement.classList.toggle('open')">
        <div class="ghdr-l">
          <span class="mbadge ${bc}">${bl}</span>
          <span class="mkey" title="${g.match_key}">${g.match_key}</span>
        </div>
        <div class="ghdr-r">
          <div class="gpills">
            <span class="gpill">${g.record_count} records</span>
            <span class="gpill${cross?" x":""}">${cross?`⚡ ${g.file_count} files`:"1 file"}</span>
            ${cu.role==="admin"&&(g.uploaders||[]).length>1?`<span class="gpill x">👥 ${g.uploaders.length} users</span>`:""}
          </div>
          <svg class="chev" width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>
        </div>
      </div>
      ${ftags?`<div class="ftags">${ftags}</div>`:""}
      <div class="gbody">
        <div class="rtw">
          <table class="rtable" data-gkey='${g.raw_key}' data-gtype='${g.match_type}' data-cols='${JSON.stringify(cols)}' data-ff='${ff}' data-recpage='1'>
            <thead><tr><th>Source File</th>${cols.map(c=>`<th>${c.replace(/_/g," ").toUpperCase()}</th>`).join("")}${adminTH}</tr></thead>
            <tbody>${rowsHTML}${smr}</tbody>
          </table>
        </div>
      </div>`;
    return card;
}

function mkRow(r,cols,ff){
    const same=r.filename===ff;
    const chip=`<span class="schip ${same?"sc-same":"sc-diff"}" title="${r.filename}">📄 ${r.filename.length>30?r.filename.slice(0,28)+"…":r.filename}</span>`;
    const cells=cols.map(c=>{const v=r.data?.[c];const d=(v===null||v===undefined||v==="nan"||v==="")?"—":String(v);return`<td title="${d}">${d.length>28?d.slice(0,26)+"…":d}</td>`;}).join("");
    const aCell=cu.role==="admin"?`<td style="font-size:12px;color:#94a3b8;">—</td>`:"";
    return`<tr><td style="min-width:160px;">${chip}</td>${cells}${aCell}</tr>`;
}

async function expandRows(btn){
    const t=btn.closest("table");
    const gkey=t.dataset.gkey, gtype=t.dataset.gtype;
    const cols=JSON.parse(t.dataset.cols), ff=t.dataset.ff;
    let recPage=parseInt(t.dataset.recpage||"1");

    btn.textContent="Loading…";btn.disabled=true;

    const p=new URLSearchParams();
    p.append("group_key",gkey);p.append("match_type",gtype);
    p.append("page",recPage);p.append("page_size",100);
    if(selUp)p.append("upload_id",selUp);
    if(selUsr)p.append("user_id",selUsr);

    const r=await af("/related-group-records?"+p.toString());
    if(!r.ok){btn.textContent="Error loading";btn.disabled=false;return;}
    const d=await r.json();

    // Remove the show-more row
    const smrRow=t.querySelector(".smr-row");if(smrRow)smrRow.remove();

    // Append new rows
    const tbody=t.querySelector("tbody");
    d.records.forEach(rec=>tbody.insertAdjacentHTML("beforeend",mkRow(rec,cols,ff)));

    recPage++;
    t.dataset.recpage=recPage;
    const loaded=tbody.querySelectorAll("tr").length;
    const remaining=d.total-loaded;

    if(remaining>0){
        tbody.insertAdjacentHTML("beforeend",`<tr class="smr-row"><td colspan="${cols.length+1+(cu.role==="admin"?1:0)}">
            <button class="smr-btn" onclick="expandRows(this)">Show ${Math.min(remaining,100)} more of ${remaining} remaining ▾</button>
        </td></tr>`);
    }
}

/* ── BY FILE ── */
async function loadFileView(area){
    const r=await af("/related-by-file?"+bp());
    if(!r.ok){toast("Failed to load","error");area.innerHTML="";return;}
    const d=await r.json();
    const tot=d.total_files, tp=Math.ceil(tot/FS), st=(page-1)*FS;
    setRI(tot,st,Math.min(st+FS,tot),"file");
    if(!d.files.length){area.innerHTML=emptyHTML("No files with related records","No duplicate contacts found in any file with the current filters.");return;}
    const adminH=cu.role==="admin"?"<th>Uploaded By</th>":"";
    let html=`<div class="ftw"><table class="ftable"><thead><tr><th>File</th>${adminH}<th>Total Records</th><th>Dup Records</th><th>Groups</th><th>Dup Rate</th><th>Actions</th></tr></thead><tbody>`;
    d.files.forEach(f=>{
        const pct=f.total_records>0?Math.round(f.dup_records/f.total_records*100):0;
        const aCell=cu.role==="admin"?`<td><span style="font-size:12px;color:#64748b;">👤 ${f.uploader}</span></td>`:"";
        html+=`<tr>
          <td><div style="display:flex;align-items:center;gap:8px;"><span style="font-size:17px;flex-shrink:0;">📄</span>
            <div><div style="font-weight:600;font-size:13.5px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;max-width:260px;" title="${f.filename}">${f.filename}</div>
            <div style="font-size:11.5px;color:#94a3b8;margin-top:2px;">${f.category}</div></div></div></td>
          ${aCell}
          <td style="font-weight:600;">${fmt(f.total_records)}</td>
          <td style="font-weight:600;color:#dc2626;">${fmt(f.dup_records)}</td>
          <td><span style="background:#ede9fe;color:#6d28d9;padding:3px 10px;border-radius:20px;font-size:12px;font-weight:700;">${fmt(f.group_count)}</span></td>
          <td><div class="dbw"><div class="dbb"><div class="dbf" style="width:${Math.min(pct,100)}%;"></div></div><span class="dpct">${pct}%</span></div></td>
          <td><a class="vfbtn" href="/related.html?upload_id=${f.upload_id}" target="_blank">View File →</a></td>
        </tr>`;
    });
    html+=`</tbody></table></div>`;
    area.innerHTML=html;
    renderPag(tp);
}

/* ── UTILS ── */
function setRI(tot,st,en,unit){document.getElementById("ri").textContent=tot===0?`No ${unit}s found`:`Showing ${st+1}–${en} of ${tot.toLocaleString()} ${unit}${tot!==1?"s":""}`;}
function emptyHTML(h,p){return`<div class="empty"><svg width="68" height="68" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2"><circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/><line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/></svg><h3>${h}</h3><p>${p}</p></div>`;}

function setView(v){view=v;page=1;document.getElementById("vKey").classList.toggle("active",v==="key");document.getElementById("vFile").classList.toggle("active",v==="file");load();}
function setTab(t,el){tab=t;page=1;document.querySelectorAll(".tab").forEach(b=>b.classList.remove("active"));el.classList.add("active");load();}
function applyFilters(){selUp=document.getElementById("fUp").value;selUsr=document.getElementById("fUsr")?.value||"";selCat=document.getElementById("fCat")?.value||"";page=1;loadStats();load();}
function resetFilters(){
    document.getElementById("fUp").value="";const fu=document.getElementById("fUsr");if(fu)fu.value="";const fc=document.getElementById("fCat");if(fc)fc.value="";
    document.getElementById("sortSel").value="size-desc";
    selUp="";selUsr="";selCat="";tab="all";page=1;view="key";
    document.querySelectorAll(".tab").forEach(b=>b.classList.remove("active"));document.querySelector('.tab[data-t="all"]').classList.add("active");
    document.getElementById("vKey").classList.add("active");document.getElementById("vFile").classList.remove("active");
    loadStats();load();
}

function renderPag(tp){
    const pg=document.getElementById("pag");pg.innerHTML="";if(tp<=1)return;
    const prev=document.createElement("button");prev.textContent="← Prev";prev.disabled=page===1;
    prev.onclick=()=>{page--;load();scrollTo({top:0,behavior:"smooth"});};pg.appendChild(prev);
    let s=Math.max(1,page-2),e=Math.min(tp,page+2);
    if(s>1)addPB(pg,1);
    if(s>2)pg.appendChild(Object.assign(document.createElement("span"),{textContent:"…",style:"padding:0 4px;color:#94a3b8;line-height:2;"}));
    for(let i=s;i<=e;i++)addPB(pg,i);
    if(e<tp-1)pg.appendChild(Object.assign(document.createElement("span"),{textContent:"…",style:"padding:0 4px;color:#94a3b8;line-height:2;"}));
    if(e<tp)addPB(pg,tp);
    const next=document.createElement("button");next.textContent="Next →";next.disabled=page===tp;
    next.onclick=()=>{page++;load();scrollTo({top:0,behavior:"smooth"});};pg.appendChild(next);
}
function addPB(pg,n){const b=document.createElement("button");b.textContent=n;if(n===page)b.classList.add("active");b.onclick=()=>{page=n;load();scrollTo({top:0,behavior:"smooth"});};pg.appendChild(b);}

init();