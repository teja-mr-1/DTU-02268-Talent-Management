// Shared fixture keeps the overview and distribution pages consistent.
const applications = [
  {id: 'APP-001', name: 'Emma Jensen', position: 'PhD student'},
  {id: 'APP-002', name: 'Oliver Hansen', position: 'PhD student'},
  {id: 'APP-003', name: 'Sofia Andersen', position: 'PhD student'},
  {id: 'APP-004', name: 'Noah Pedersen', position: 'Associate professor'},
  {id: 'APP-005', name: 'Freja Madsen', position: 'Associate professor'},
  {id: 'APP-006', name: 'William Olsen', position: 'Assistant professor'}
];
const employees = ['Anna Nielsen', 'Lars Petersen', 'Maria Larsen'];
const status = document.querySelector('#status');

function renderRows(target, choosingReviewers) {
  for (const application of applications) {
    const row = document.createElement('tr');
    for (const value of [application.id, application.name, application.position]) {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.append(cell);
    }
    const cell = document.createElement('td');
    if (choosingReviewers) {
      const select = document.createElement('select');
      select.name = application.id;
      select.required = true;
      select.setAttribute('aria-label', `Reviewer for ${application.name} (${application.id})`);
      select.add(new Option('Choose an employee', ''));
      for (const employee of employees) select.add(new Option(employee, employee));
      cell.append(select);
    } else {
      cell.textContent = 'Pending';
    }
    row.append(cell);
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
  const form = document.querySelector('#distribution-form');
  const confirm = document.querySelector('#confirm');
  form.addEventListener('submit', async event => {
    event.preventDefault();
    const selections = new FormData(form);
    const reviewers = form.querySelectorAll('select');
    reviewers.forEach(select => { select.disabled = true; });
    confirm.disabled = true;
    status.dataset.error = 'false';
    status.textContent = 'Confirming…';
    try {
      const response = await fetch('/applications/distribute', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({assignments: applications.map(application => ({
          applicationId: application.id, reviewer: selections.get(application.id)
        }))})
      });
      if (!response.ok) throw new Error('Request failed');
      status.textContent = 'Distribution submitted for processing.';
      confirm.textContent = 'Submitted';
    } catch {
      status.dataset.error = 'true';
      status.textContent = 'Could not confirm distribution. Please try again.';
      reviewers.forEach(select => { select.disabled = false; });
      confirm.disabled = false;
    }
  });
}
