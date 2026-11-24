// Shared fixture keeps the overview and assignment preview consistent.
const applications = [
  {id: 'APP-001', name: 'Emma Jensen', position: 'PhD student', reviewer: 'Anna Nielsen'},
  {id: 'APP-002', name: 'Oliver Hansen', position: 'PhD student', reviewer: 'Lars Petersen'},
  {id: 'APP-003', name: 'Sofia Andersen', position: 'PhD student', reviewer: 'Maria Larsen'},
  {id: 'APP-004', name: 'Noah Pedersen', position: 'Associate professor', reviewer: 'Anna Nielsen'},
  {id: 'APP-005', name: 'Freja Madsen', position: 'Associate professor', reviewer: 'Lars Petersen'},
  {id: 'APP-006', name: 'William Olsen', position: 'Assistant professor', reviewer: 'Maria Larsen'}
];
const status = document.querySelector('#status');

function renderRows(target, assigned) {
  for (const application of applications) {
    const row = document.createElement('tr');
    for (const value of [application.id, application.name, application.position,
      assigned ? application.reviewer : 'Pending']) {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.append(cell);
    }
    target.append(row);
  }
}

const queue = document.querySelector('#applications');
if (queue) {
  renderRows(queue, false);
  const summary = document.querySelector('#summary');
  for (const position of ['All pending', 'PhD student', 'Associate professor', 'Assistant professor']) {
    const card = document.createElement('article');
    const label = document.createElement('span');
    label.textContent = position;
    const count = document.createElement('strong');
    count.textContent = position === 'All pending' ? applications.length :
      applications.filter(application => application.position === position).length;
    card.append(label, count);
    summary.append(card);
  }
  fetch('/applications?status=pending')
    .then(response => {
      if (!response.ok) throw new Error('Request failed');
      status.textContent = 'Application queue ready.';
    })
    .catch(() => {
      status.dataset.error = 'true';
      status.textContent = 'Could not reach the backend. Please try again later.';
    });
}

const assignments = document.querySelector('#assignments');
if (assignments) {
  renderRows(assignments, true);
  const confirm = document.querySelector('#confirm');
  confirm.addEventListener('click', async () => {
    confirm.disabled = true;
    status.dataset.error = 'false';
    status.textContent = 'Confirming…';
    try {
      const response = await fetch('/applications/distribute', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({assignments: applications.map(application => ({
          applicationId: application.id, reviewer: application.reviewer
        }))})
      });
      if (!response.ok) throw new Error('Request failed');
      status.textContent = 'Distribution confirmed.';
      confirm.textContent = 'Confirmed';
    } catch {
      status.dataset.error = 'true';
      status.textContent = 'Could not confirm distribution. Please try again.';
      confirm.disabled = false;
    }
  });
}
