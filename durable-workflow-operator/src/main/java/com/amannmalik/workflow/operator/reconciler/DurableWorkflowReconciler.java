package com.amannmalik.workflow.operator.reconciler;

import com.amannmalik.workflow.operator.model.DurableWorkflow;
import com.amannmalik.workflow.operator.model.DurableWorkflowStatus;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ControllerConfiguration
public class DurableWorkflowReconciler implements Reconciler<DurableWorkflow> {
    private static final Logger log = LoggerFactory.getLogger(DurableWorkflowReconciler.class);

    @Override
    public UpdateControl<DurableWorkflow> reconcile(DurableWorkflow desired, Context context) {
        var client = context.getClient();
        var name = desired.getMetadata().getName();
        var namespace = java.util.Optional.ofNullable(desired.getMetadata().getNamespace())
                        .filter(s -> !s.isBlank())
                        .orElse("default");

        Deployment existing = client.apps().deployments().inNamespace(namespace).withName(name).get();
        if (existing == null) {
            var deployment = new Deployment();
            deployment.setMetadata(new io.fabric8.kubernetes.api.model.ObjectMeta());
            deployment.getMetadata().setName(name);
            deployment.getMetadata().setNamespace(namespace);
            var spec = new io.fabric8.kubernetes.api.model.apps.DeploymentSpec();
            var template = new io.fabric8.kubernetes.api.model.PodTemplateSpec();
            template.setMetadata(new io.fabric8.kubernetes.api.model.ObjectMeta());
            template.getMetadata().setLabels(java.util.Map.of("app", name));
            var podSpec = new io.fabric8.kubernetes.api.model.PodSpec();
            var container = new io.fabric8.kubernetes.api.model.Container();
            container.setName("runtime");
            container.setImage("durable-workflow-runtime:latest");
            podSpec.setContainers(java.util.List.of(container));
            template.setSpec(podSpec);
            spec.setSelector(new io.fabric8.kubernetes.api.model.LabelSelector(null, java.util.Map.of("app", name)));
            spec.setTemplate(template);
            deployment.setSpec(spec);
            client.resource(deployment).create();
        }
        context.getSecondaryResource(Deployment.class);
        if (desired.getStatus() == null) {
            var status = new DurableWorkflowStatus();
            desired.setStatus(status);
            return UpdateControl.patchStatus(desired);
        }
        return UpdateControl.noUpdate();
    }
}
