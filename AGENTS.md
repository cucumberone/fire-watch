# Fire-Watch Event Server

This repo sets up an event server.

## Tech Stack

- **No data loss.** Dataloss is not accepted.
- **OpenShift** as the target platform.
- **100% H/A** for every component, and it must handle split brain for every component.
  - 3 replicas on different nodes.
  - Must handle eviction / deletion of pods etc.
- **Dragonfly operator is off the table.** Make it work with manifests.
- **WebSocket node deployment** acting as server for web clients. Can scale with HPA or whatever it is called.
- **React frontend** talking to the node websocket server for visualization.

## Deployment

- Create manifests that fit OpenShift (i.e. **routes instead of ingress**) in the `k8s` directory.
- **RabbitMQ:** 3 replicas of the latest RabbitMQ server with quorum queues and affinity so they never run on the same node.
- **DragonflyDB:** 3 replicas. They need the "always find 2 replicas" setting to receive writes — split brain protection.
- **Workers:** 3 replicas getting messages from RabbitMQ. For now, write a small JavaScript service that writes whatever comes to the worker into Dragonfly.
- **Probes:** if you see the need for readiness probes, create them and handle them.
- **Dragonfly persistence:** use the AOF feature, but every hour refresh that file with a new one from what's left in Dragonfly. This is to make restart and reread fast.
- **Redis Sentinels:** needed for the workers, I think.
- **Sizing.**

## Plan

- Refine this spec and put the different components in their own `.md` files in the `docs` directory, then just link them in `AGENTS.md`.
- Always update the relevant files under `docs` whenever changes are made.

## Guidelines

- Never commit or push until I say so.
- Keep the returning information at a minimum.

## Agent Directives & Behavior

### Output Style & Brevity Rules

- **Direct Answers First:** Never begin a response with a conversational preamble, acknowledgment, or filler phrases (e.g., "Sure, I can help with that", "Based on your request", "Here is"). Start immediately with the answer or tool output summary.

- **Maximum Conciseness:** Eliminate all redundant text, conversational wrap-ups, and unrequested code summaries. Provide only the requested code, fix, or data.

- **Strict Conversational Ban:** Do not feign human emotions, offer pleasantries, or apologize unless a critical execution error occurs. Keep the tone clinical, objective, and developer-centric.

- **Code-First Hierarchy:** When answering code queries, output the copy-pasteable snippet immediately. Place brief bulleted explanations *below* the code block, never above it.

- **No Conceptual Lectures:** Do not explain the history, theory, or architectural concepts of a technology unless explicitly asked to define or explain it.