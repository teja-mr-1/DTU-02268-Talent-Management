import { XMLParser } from "fast-xml-parser";
import type { DCRGraphS, EventMap, SubProcess } from "./dcr-engine/types";
import { isSubProcess } from "./dcr-engine/types";
import { parseDurationMs } from "./dcr-engine/utility";

const arrayTags = new Set([
  "dcr:event",
  "dcr:nesting",
  "dcr:subProcess",
  "dcr:relation",
  "event",
  "nesting",
  "subProcess",
  "relation",
]);

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  removeNSPrefix: true,
  parseAttributeValue: false,
  isArray: (tagName: string) => arrayTags.has(tagName),
});

interface ParsedModel {
  graph: DCRGraphS;
  hasSubProcesses: boolean;
}

function asArray<T>(val: T | T[] | undefined): T[] {
  if (!val) return [];
  return Array.isArray(val) ? val : [val];
}

function boolAttr(val: unknown): boolean {
  return val === true || val === "true";
}

function collectNestingChildren(node: any, map: Map<string, string[]>): void {
  for (const nest of asArray(node.nesting)) {
    const nestId = String(nest["@_id"]);
    const directChildren: string[] = [];
    for (const ev of asArray(nest.event)) {
      directChildren.push(String(ev["@_id"]));
    }
    for (const sp of asArray(nest.subProcess)) {
      directChildren.push(String(sp["@_id"]));
    }
    for (const subNest of asArray(nest.nesting)) {
      directChildren.push(String(subNest["@_id"]));
    }
    map.set(nestId, directChildren);
    collectNestingChildren(nest, map);
  }
  for (const sp of asArray(node.subProcess)) {
    collectNestingChildren(sp, map);
  }
}

function expandNestingIds(id: string, nestings: Map<string, string[]>): string[] {
  const children = nestings.get(id);
  if (!children) return [id];
  return children.flatMap((child) => expandNestingIds(child, nestings));
}

function addEventsAndSubProcesses(
  graph: DCRGraphS,
  parent: DCRGraphS | SubProcess,
  container: any
): void {
  for (const ev of asArray(container.event)) {
    const eventId = String(ev["@_id"]);
    const label = ev["@_description"] != null ? String(ev["@_description"]) : eventId;
    const role = ev["@_role"] != null ? String(ev["@_role"]) : "";

    parent.events.add(eventId);
    graph.labels.add(label);
    graph.labelMap[eventId] = label;
    if (!graph.labelMapInv[label]) graph.labelMapInv[label] = new Set();
    graph.labelMapInv[label].add(eventId);
    graph.roles.add(role);
    graph.roleMap[eventId] = role;
    if (isSubProcess(parent)) graph.subProcessMap[eventId] = parent;

    if (boolAttr(ev["@_pending"])) graph.marking.pending.set(eventId, undefined);
    if (boolAttr(ev["@_executed"])) graph.marking.executed.set(eventId, {});
    if (boolAttr(ev["@_included"])) graph.marking.included.add(eventId);

    graph.conditionsFor[eventId] = new Set();
    graph.milestonesFor[eventId] = new Set();
    graph.responseTo[eventId] = new Set();
    graph.includesTo[eventId] = new Set();
    graph.excludesTo[eventId] = new Set();
  }

  for (const nest of asArray(container.nesting)) {
    addEventsAndSubProcesses(graph, parent, nest);
  }

  for (const sp of asArray(container.subProcess)) {
    const spId = String(sp["@_id"]);
    const label = sp["@_description"] != null ? String(sp["@_description"]) : "";
    parent.events.add(spId);
    const subProcess: SubProcess = { id: spId, parent, events: new Set() };
    graph.subProcesses[spId] = subProcess;
    graph.labelMap[spId] = label;
    if (!graph.labelMapInv[label]) graph.labelMapInv[label] = new Set();
    graph.labelMapInv[label].add(spId);
    if (boolAttr(sp["@_pending"])) graph.marking.pending.set(spId, undefined);
    if (boolAttr(sp["@_executed"])) graph.marking.executed.set(spId, {});
    if (boolAttr(sp["@_included"])) graph.marking.included.add(spId);

    graph.conditionsFor[spId] = new Set();
    graph.milestonesFor[spId] = new Set();
    graph.responseTo[spId] = new Set();
    graph.includesTo[spId] = new Set();
    graph.excludesTo[spId] = new Set();

    addEventsAndSubProcesses(graph, subProcess, sp);
  }
}

function addRelationPairs(
  relationSet: EventMap,
  fromIds: string[],
  toIds: string[]
): void {
  for (const s of fromIds) {
    if (!relationSet[s]) relationSet[s] = new Set();
    for (const t of toIds) {
      relationSet[s].add(t);
    }
  }
}

export function parseDcrEditorXml(xml: string): ParsedModel {
  const parsed = xmlParser.parse(xml);
  const dcrGraph = parsed?.definitions?.dcrGraph;
  if (!dcrGraph) {
    throw new Error("Invalid DCR Editor XML: missing <dcr:dcrGraph>");
  }

  const graph: DCRGraphS = {
    events: new Set(),
    labels: new Set(),
    labelMap: {},
    labelMapInv: {},
    roles: new Set(),
    roleMap: {},
    subProcesses: {},
    subProcessMap: {},
    conditionsFor: {},
    milestonesFor: {},
    responseTo: {},
    includesTo: {},
    excludesTo: {},
    marking: {
      executed: new Map(),
      included: new Set(),
      pending: new Map(),
    },
  };

  const nestings = new Map<string, string[]>();
  collectNestingChildren(dcrGraph, nestings);
  addEventsAndSubProcesses(graph, graph, dcrGraph);

  for (const rel of asArray(dcrGraph.relation)) {
    const sourceRef = String(rel["@_sourceRef"]);
    const targetRef = String(rel["@_targetRef"]);
    const relType = String(rel["@_type"]);
    const sources = expandNestingIds(sourceRef, nestings);
    const targets = expandNestingIds(targetRef, nestings);

    switch (relType) {
      case "condition":
        addRelationPairs(graph.conditionsFor, targets, sources);
        break;
      case "milestone":
        addRelationPairs(graph.milestonesFor, targets, sources);
        break;
      case "response":
        addRelationPairs(graph.responseTo, sources, targets);
        break;
      case "include":
        addRelationPairs(graph.includesTo, sources, targets);
        break;
      case "exclude":
        addRelationPairs(graph.excludesTo, sources, targets);
        break;
    }

    const guard = rel["@_guard"] ? String(rel["@_guard"]) : undefined;
    if (guard) {
      if (!graph.guardMap) graph.guardMap = {};
      for (const s of sources) {
        if (!graph.guardMap[s]) graph.guardMap[s] = {};
        for (const t of targets) {
          if (!graph.guardMap[s][t]) graph.guardMap[s][t] = {};
          graph.guardMap[s][t][relType] = guard;
        }
      }
    }

    const time = rel["@_time"] ? String(rel["@_time"]) : undefined;
    if (time && (relType === "condition" || relType === "response")) {
      const ms = parseDurationMs(time);
      if (ms > 0) {
        if (!graph.timeConstraintMap) graph.timeConstraintMap = {};
        for (const s of sources) {
          if (!graph.timeConstraintMap[s]) graph.timeConstraintMap[s] = {};
          for (const t of targets) {
            if (!graph.timeConstraintMap[s][t]) graph.timeConstraintMap[s][t] = {};
            if (relType === "condition") {
              const existing = graph.timeConstraintMap[s][t].delay;
              graph.timeConstraintMap[s][t].delay = existing !== undefined ? Math.max(existing, ms) : ms;
            }
            if (relType === "response") {
              const existing = graph.timeConstraintMap[s][t].deadline;
              graph.timeConstraintMap[s][t].deadline = existing !== undefined ? Math.min(existing, ms) : ms;
            }
          }
        }
      }
    }
  }

  return {
    graph,
    hasSubProcesses: Object.keys(graph.subProcesses).length > 0,
  };
}
