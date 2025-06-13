# durable-workflow-runtime

This project combines the following technologies:

- [Serverless Workflow specification](https://github.com/serverlessworkflow/specification)
- [Restate](https://github.com/restatedev/restate)

into a runtime that is configured with a workflow definition and starts executing its tasks while it uses the journaling
capabilities of Restate to keep track of everything.
