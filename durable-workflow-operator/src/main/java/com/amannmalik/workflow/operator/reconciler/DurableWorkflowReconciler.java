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
        // TODO: load base manifests and compare
        context.getSecondaryResource(Deployment.class);
        if (desired.getStatus() == null) {
            var status = new DurableWorkflowStatus();
            desired.setStatus(status);
            return UpdateControl.patchStatus(desired);
        }
        return UpdateControl.noUpdate();
    }
}
