const form = document.querySelector('#application');
const status = document.querySelector('#status');
form.addEventListener('submit', async event => {
  event.preventDefault();
  const button = form.querySelector('button');
  button.disabled = true;
  status.textContent = 'Sending…';
  status.dataset.error = 'false';
  try {
    const response = await fetch('/api/applications', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name: form.elements.namedItem('name').value.trim(), position: form.elements.namedItem('position').value})
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'Submission failed');
    status.textContent = `Accepted into Flink's queue. Reference: ${result.applicationId}. Camunda processing follows asynchronously.`;
    form.reset();
  } catch (error) {
    status.dataset.error = 'true';
    status.textContent = `${error.message}. If the connection failed, delivery is uncertain; retrying may submit twice.`;
  } finally { button.disabled = false; }
});
