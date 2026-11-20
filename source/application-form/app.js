const appForm = document.getElementById('applicationForm');
const appStatus = document.getElementById('appStatus');
const hrStatus = document.getElementById('hrStatus');
const hrVacancyInput = document.getElementById('hrVacancyId');
const vacanciesList = document.getElementById('vacanciesList');

async function submitApplication(payload) {
  const res = await fetch('/api/applications', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  return res.json();
}

appForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  const data = Object.fromEntries(new FormData(appForm).entries());
  const out = await submitApplication(data);
  appStatus.textContent = out.applicationId
    ? `Accepted ${out.applicationId.slice(0, 8)} for ${out.vacancyId}`
    : (out.error || 'Submission failed');
  refreshOverview();
});

document.getElementById('spikeBtn').addEventListener('click', async () => {
  const vacancyId = document.getElementById('appVacancyId').value.trim() || 'VAC-FP-2026-01';
  appStatus.textContent = 'Sending 6 rapid applications into Flink RollingHour...';
  for (let i = 1; i <= 6; i++) {
    await submitApplication({
      name: `Spike Candidate #${i}`,
      position: 'Full professor',
      vacancyId
    });
  }
  appStatus.textContent = `Sent 6 applications for ${vacancyId} (triggers Flink scale-up > 5/hr).`;
  setTimeout(refreshOverview, 500);
});

document.querySelectorAll('[data-scenario]').forEach((btn) => {
  btn.addEventListener('click', async () => {
    const scenario = btn.dataset.scenario;
    const vacancyId = (hrVacancyInput.value.trim() || 'VAC-FP-2026-01') + '-' + Math.floor(Math.random() * 90 + 10);
    hrStatus.textContent = `Queued ${scenario} for ${vacancyId}... waiting for Flink + Process Doctor...`;
    const res = await fetch(`/api/vacancies/${encodeURIComponent(vacancyId)}/simulate/${encodeURIComponent(scenario)}`, {
      method: 'POST'
    });
    const out = await res.json();
    hrStatus.textContent = out.eventsQueued
      ? `Queued ${out.eventsQueued} events for ${out.vacancyId} (${scenario}). Process Doctor audit running...`
      : (out.error || 'Failed');
    refreshOverview();
  });
});

document.querySelectorAll('.step-btn').forEach((btn) => {
  btn.addEventListener('click', async () => {
    const vacancyId = hrVacancyInput.value.trim() || 'VAC-FP-2026-01';
    const payload = {
      eventName: btn.dataset.event,
      role: btn.dataset.role,
      terminal: btn.dataset.terminal === 'true'
    };
    const res = await fetch(`/api/vacancies/${encodeURIComponent(vacancyId)}/events`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const out = await res.json();
    hrStatus.textContent = out.eventName
      ? `Sent "${out.eventName}" (${out.role}) to ${out.vacancyId}${out.terminal ? ' [FINAL -> running Process Doctor]' : ''}`
      : (out.error || 'Failed');
    refreshOverview();
  });
});

document.getElementById('refreshBtn').addEventListener('click', refreshOverview);

function escapeHtml(s) {
  return String(s ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

async function refreshOverview() {
  try {
    const res = await fetch('/api/vacancies');
    const data = await res.json();

    document.getElementById('modePill').textContent = `Mode: ${data.camundaMode}`;
    document.getElementById('appCount').textContent = data.totalApplications ?? 0;
    document.getElementById('spikeCount').textContent = (data.scaleUpAlerts || []).length;

    const vacancies = data.vacancies || [];
    if (!vacancies.length) {
      vacanciesList.innerHTML = '<p class="muted">No vacancy traces yet. Run a scenario or send events above.</p>';
      return;
    }

    vacanciesList.innerHTML = vacancies.map((v) => {
      const conf = v.conformanceResult;
      const confHtml = conf
        ? `<div class="conf-box">
             <strong>Process Doctor Audit:</strong>
             Conforming: <b>${conf.conforming}</b> |
             Partially Violating: <b>${conf.partiallyViolating}</b> |
             Violating: <b>${conf.violating}</b> |
             Structural Violations: <b>${conf.totalStructuralViolations}</b>
             <br><span class="muted">Model: ${escapeHtml(conf.modelName)} | Checked: ${escapeHtml(conf.checkedAt)}</span>
           </div>`
        : (v.errorMessage
            ? `<div class="conf-box" style="color:#c5221f">Error: ${escapeHtml(v.errorMessage)}</div>`
            : '');

      const eventsHtml = (v.events || []).map((e) =>
        `<li><b>${escapeHtml(e.eventName)}</b> <span class="muted">(${escapeHtml(e.role)})${e.terminal ? ' [FINAL]' : ''}</span></li>`
      ).join('');

      const xesHtml = v.xesXml
        ? `<details><summary>View Generated XES Log (${escapeHtml(v.xesPath)})</summary><pre>${escapeHtml(v.xesXml)}</pre></details>`
        : '';

      return `<div class="vac-card">
        <div class="vac-head">
          <strong>${escapeHtml(v.vacancyId)}</strong>
          <span class="badge ${escapeHtml(v.status)}">${escapeHtml(v.status)}</span>
        </div>
        <ol class="events-list">${eventsHtml}</ol>
        ${confHtml}
        ${xesHtml}
      </div>`;
    }).join('');
  } catch (err) {
    console.error('Refresh error', err);
  }
}

refreshOverview();
setInterval(refreshOverview, 2500);
