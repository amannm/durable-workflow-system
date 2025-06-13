# durable-workflow-system

This project combines the following technologies:

- [Serverless Workflow specification](https://github.com/serverlessworkflow/specification)
- [Restate](https://github.com/restatedev/restate)
- [Java Operator SDK](https://github.com/operator-framework/java-operator-sdk)

into a system that accepts a workflow definition, deploys it on Kubernetes, starts executing the tasks while it uses the
journaling capabilities of Restate to keep track of everything.
