import { signedBuzzRequest } from '/buzz-knowledge.mjs';
const $ = id => document.getElementById(id);
let channels=[], channel, selected, pubkey;
let submissionId=crypto.randomUUID();
let answeringQuestion=null;
const message = text => { $('message').textContent=text; };
async function api(path,payload={}) {
  if (!window.nostr?.signEvent) throw new Error('An existing Nostr browser signer is required. Your private key stays with that signer.');
  return signedBuzzRequest({origin:location.origin,path:'/api/workspace/'+path,
    payload:{...(channel?{channel_id:channel.id}:{}),...payload},signEvent: event=>window.nostr.signEvent(event)});
}
function action(id,fn) { $(id).onclick=async()=>{ $(id).disabled=true;try{await fn();}catch(e){message(e.message);}finally{$(id).disabled=false;} }; }
function row(parent,title,subtitle,label,fn) {
  const r=document.createElement('div');r.className='row';const text=document.createElement('div');
  const h=document.createElement('strong');h.textContent=title;const p=document.createElement('div');p.className='muted';p.textContent=subtitle;text.append(h,p);r.append(text);
  if(fn){const b=document.createElement('button');b.textContent=label;b.onclick=()=>fn().catch(e=>message(e.message));r.append(b);}parent.append(r);
}
function show(view){for(const id of ['knowledge','ask','unanswered','propose','jobs'])$(id).classList.toggle('hidden',id!==view);$('detail').classList.add('hidden');}
for(const button of document.querySelectorAll('[data-view]'))button.onclick=()=>{if(button.dataset.view==='propose')answer(null);show(button.dataset.view);if(button.dataset.view==='unanswered')unanswered().catch(e=>message(e.message));};
async function library(){if(!channel)return;const docs=await api('documents');$('documents').replaceChildren();
  for(const d of docs){const due=d.review_due?new Date(d.review_due):null;row($('documents'),d.title,
    `${d.state||'Needs review'} · ${d.owner?'Owner assigned':'No owner'}${due?` · ${due<Date.now()?'Review overdue':'Review due'} ${due.toLocaleDateString()}`:''}`,'Open',()=>open(d.id));}
  if(!docs.length)message('No knowledge in this channel yet.');else message(`${docs.length} documents loaded.`);
}
async function open(id){selected=await api('detail',{document_id:id});const d=selected.document;const m=typeof d.metadata==='string'?JSON.parse(d.metadata):d.metadata;
  $('detail').classList.remove('hidden');$('detail-title').textContent=d.title;$('detail-meta').textContent=`Document ${d.id} · ${d.source_uri||'No source link'}`;
  $('evidence').textContent=selected.chunks.map(c=>`[Chunk ${c.ordinal+1}]\n${c.content}`).join('\n\n');
  $('review-controls').classList.toggle('hidden',!['owner','admin'].includes(channel.role));$('owner').value=m.knowledge_owner||pubkey;
  $('state').value=m.knowledge_state||'proposed';$('due').value=(m.knowledge_review_due||new Date(Date.now()+90*86400000).toISOString()).slice(0,10);
  $('readers').value=(m.knowledge_readers||[]).join('\n');$('replacement').value=m.superseded_by_document_id||'';$('history').textContent=JSON.stringify({reviews:selected.history,feedback:selected.feedback},null,2);
}
action('connect',async()=>{if(!window.nostr)throw new Error('Connect an existing Nostr browser signer first.');pubkey=await window.nostr.getPublicKey();channels=await api('channels');
  $('channels').replaceChildren();for(const c of channels){const o=document.createElement('option');o.value=c.id;o.textContent=c.name;$('channels').append(o);} $('channels').disabled=!channels.length;
  channel=channels[0];$('role').textContent=channel?.role||'';await library();if(!channel)message('No active Buzz channel memberships were found.');});
$('channels').onchange=async()=>{channel=channels.find(c=>c.id===$('channels').value);selected=null;$('detail').classList.add('hidden');$('role').textContent=channel.role;try{await library();}catch(e){message(e.message);}};
action('refresh',library);
function answer(q){answeringQuestion=q;$('answering').classList.toggle('hidden',!q);$('answering').textContent=q?`Answering: "${q.question}". The question is marked answered when this proposal is ingested; it still needs review before it is used in answers.`:'';if(q&&!$('title').value)$('title').value=q.question.slice(0,300);}
async function unanswered(){if(!channel)return;const list=await api('unanswered',{status:$('unanswered-status').value});$('unanswered-list').replaceChildren();
  const admin=['owner','admin'].includes(channel.role),contributor=admin||channel.role==='member';
  for(const q of list){const state=q.status==='answered'?` · Answered${q.answer_title?`: ${q.answer_title} (${q.answer_state||'unknown'})`:''}`:q.status==='dismissed'?` · Dismissed${q.resolution_note?`: ${q.resolution_note}`:''}`:'';
    row($('unanswered-list'),q.question,`Asked ${q.asked_count} time${q.asked_count===1?'':'s'} by ${q.distinct_askers} ${q.distinct_askers===1?'person':'people'} · last ${new Date(q.last_asked_at).toLocaleString()}${state}`,
      'Answer',contributor&&q.status==='open'?async()=>{answer(q);show('propose');}:null);
    if(admin){const r=$('unanswered-list').lastChild,b=document.createElement('button');b.textContent=q.status==='open'?'Dismiss':'Reopen';
      b.onclick=async()=>{try{const note=q.status==='open'?(prompt('Reason for dismissing (optional)')||''):'';await api('unanswered/resolve',{question_id:q.id,action:q.status==='open'?'dismissed':'reopen',note});await unanswered();}catch(e){message(e.message);}};r.append(b);}}
  message(list.length?`${list.length} questions.`:'No questions in this view.');}
action('refresh-unanswered',unanswered);
$('unanswered-status').onchange=()=>unanswered().catch(e=>message(e.message));
action('submit',async()=>{const job=await api('submit',{request_id:submissionId,title:$('title').value,text:$('proposal').value,source_uri:$('source').value||null,...(answeringQuestion?{answers_question_id:answeringQuestion.id}:{})});submissionId=crypto.randomUUID();answer(null);message(`Submission accepted. Job ${job.id}`);show('jobs');await jobs();});
async function jobs(){const list=await api('jobs');$('job-list').replaceChildren();for(const j of list){row($('job-list'),j.status,`${j.id} · Attempts ${j.attempts}${j.error?' · '+j.error:''}`,j.status==='failed'?'Retry':'Cancel',
  ['pending','failed'].includes(j.status)?async()=>{await api('job-action',{job_id:j.id,action:j.status==='failed'?'retry':'cancel'});await jobs();}:null);}}
action('refresh-jobs',jobs);
action('save-review',async()=>{await api('review',{document_id:selected.document.id,request_id:crypto.randomUUID(),expected_updated_at:selected.document.updated_at,state:$('state').value,
  owner_pubkey:$('owner').value,review_due:new Date($('due').value+'T23:59:59Z').toISOString(),note:$('review-note').value,superseded_by:$('replacement').value||null,readers:$('readers').value.trim()?$('readers').value.trim().split(/\s+/):null});message('Review saved with your verified Nostr identity.');await open(selected.document.id);});
action('send-feedback',async()=>{await api('feedback',{document_id:selected.document.id,request_id:crypto.randomUUID(),category:$('category').value,note:$('feedback-note').value});message('Feedback recorded.');await open(selected.document.id);});
action('ask-button',async()=>{if(!channel)throw new Error('Select a channel first.');const answer=await signedBuzzRequest({origin:location.origin,path:'/api/ask',payload:{channel_id:channel.id,query:$('question').value},signEvent:event=>window.nostr.signEvent(event)});
  $('answer').textContent=answer.answer+'\n\n'+answer.citations.map((c,i)=>`[${i+1}] ${c.title} · ${c.source_uri||c.document_id}`).join('\n');message('Answer uses approved knowledge in this channel.');});
