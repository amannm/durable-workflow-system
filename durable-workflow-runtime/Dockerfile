FROM docker.restate.dev/restatedev/restate:latest
COPY target/durable-workflow-runtime-0.1.0-SNAPSHOT.jar /app/runtime.jar
COPY target/lib/ /app/lib/
ENTRYPOINT ["java","-cp","/app/runtime.jar:/app/lib/*","com.amannmalik.workflow.runtime.Entrypoint"]
