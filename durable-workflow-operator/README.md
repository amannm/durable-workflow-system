# durable-workflow-operator

* accepts a custom resource containing a nested Serverless Workflow DSL object tree
* maps the custom resource to a set of kubernetes resources, with a deployment using the durable-workflow-runtime docker
  image from the other submodule in the project