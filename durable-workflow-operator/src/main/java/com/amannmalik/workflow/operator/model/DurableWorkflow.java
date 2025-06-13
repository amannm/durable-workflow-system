package com.amannmalik.workflow.operator.model;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

import java.io.Serial;

@Group("amannmalik.com")
@Version("v1alpha1")
@Kind("DurableWorkflow")
@Plural("durableworkflows")
@Singular("durableworkflow")
@ShortNames("dwf")
public class DurableWorkflow extends CustomResource<DurableWorkflowSpec, DurableWorkflowStatus> {

    @Serial
    private static final long serialVersionUID = 1L;
}
