# DTU 02268 - Project Description & Requirements

**Course:** 02268 Process-oriented and Event-driven Software Systems  
**Last Update:** September 23, 2026  
**Original PDF:** [`docs/project_description.pdf`](./project_description.pdf)

---

## 1. General Instructions & Requirements

You will implement a process-oriented application system including connection with event sources and a process aware information system (PAIS). You will identify and model processes AS-IS, and improve them making sure they have appropriate notions of quality. You will refine processes AS-IS, finding ways to improve their initial behaviour, making them ready to be executable in a PAIS. You will operate processes and generate event-logs, that you can audit using notions of compliance in DCR graphs.

### General Requirements:
* **Team:** The project must be conducted in teams of 6 persons.
* **Domain:** Identify a scenario relevant in terms of process automation and event generation/consumption, implementing it and generating logs for analysis.
* **PAIS:** The process implementation should use Process Aware Information Systems (e.g., Camunda and DCR graphs).
* **CEP:** The project should make use of a system for Complex Event Processing (e.g., Siddhi, Apache Flink).
* **Bi-directional Communication:** The project must include interaction between PAIS and CEP in **both directions** (i.e., from the CEP platform to the PAIS, and from the PAIS to the CEP platform).
* **Uniqueness:** Each group selects unique processes within their application domain.

---

## 2. Minimum Success Requirements Checklist

To be successful, the project must involve the following minimum requirements:

1. **6 Business Processes:**
   * At least **1 BPMN model**.
   * At least **1 DCR graph** (where most elements discussed during lectures are properly used).
   * Mark the student responsible for each process model in the final hand-in (each student should develop at least one process model).
2. **Knowledge-Intensive Process:** At least one process modeled should correspond to the **Knowledge-Intensive Process (KiP)** category.
3. **Execution in PAIS:** Implementation of processes in at least 1 PAIS using Camunda, and 1 PAIS using DCR graphs.
4. **Event Sources:** At least **2 external event sources**.
5. **CEP Agents:** Processing of events with proper CEP agents (e.g., Siddhi or Apache Flink).
6. **Bi-directional Interaction:** Both PAIS $\rightarrow$ CEP and CEP $\rightarrow$ PAIS communication.
7. **Compliance Rules:** One set of compliance rules for each of the implemented process models (modeled as DCR graphs).
8. **Conformance Checking:** Analysis of compliance checking of executions via event-log generation and conformance checking (using `processdoctor.dtu.dk`).

---

## 3. Project Chosen: Theme 5 - Talent Management Processes

Universities run multiple processes including teaching, research, innovation, and management. Part of the pledges of the university is to show transparency in its hiring and career progression processes, illustrating that decisions being taken are inclusive and merit based.

### Goals for Theme 5:
1. **Discover:** The most important HR processes included in talent management of personnel (inspired by DTU Compute processes).
2. **Improve:** Illustrate alternatives on how processes could be improved (TO-BE models).
3. **Simulate:** Develop a simulator for a case management system to support HR administration in managing hirings and progression plans.
4. **Events:** CEP events will simulate real workplace occurrences (e.g., new job applications, changes of role, increased absences, performance reviews, etc.).

---

## 4. Key Deadlines & Deliverables

| Milestone | Date | Description |
| :--- | :--- | :--- |
| **Topic Decision** | 12 October 2026 | Register topic on DTU Learn |
| **Intermediate Presentation** | **3 November 2026** | Mandatory status presentation (10-15 min + 3 min Q&A, not graded) |
| **Final Submission** | **15 December 2026, 23:59** | Complete package (.zip) containing report, source, and slides |
| **Final Presentation** | TBA (Exam period) | 10-minute presentation per group + individual oral Q&A |

### What to Submit (Single .zip file containing):
1. **`report.pdf`:** Max 10 pages long, structured according to `demo-report.pdf`. Must include a link to a screencast video (max 3 minutes) demonstrating all implemented functionalities.
2. **`source.zip`:** Process definitions (.bpmn or DCR XML, plus .pdf diagrams), CEP agent source code, service implementations (e.g. Java/Python), and PAIS customizations.
3. **`presentation.pdf`:** Exported slide deck for the final presentation.
