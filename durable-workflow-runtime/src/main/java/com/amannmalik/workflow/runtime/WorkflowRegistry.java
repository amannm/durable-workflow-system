package com.amannmalik.workflow.runtime;

import io.serverlessworkflow.api.types.Document;
import io.serverlessworkflow.api.types.Workflow;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple in-memory registry to look up workflow definitions by namespace, name and version.
 */
public final class WorkflowRegistry {

    private static final Map<String, Workflow> REGISTRY = new ConcurrentHashMap<>();

    private WorkflowRegistry() {
    }

    private static String key(String namespace, String name, String version) {
        return namespace + ":" + name + ":" + version;
    }

    /**
     * Register a workflow in the registry.
     */
    public static void register(Workflow workflow) {
        Document doc = workflow.getDocument();
        if (doc != null) {
            REGISTRY.put(key(doc.getNamespace(), doc.getName(), doc.getVersion()), workflow);
        }
    }

    /**
     * Retrieve a workflow from the registry.
     */
    public static Workflow get(String namespace, String name, String version) {
        return REGISTRY.get(key(namespace, name, version));
    }

    /**
     * Remove all registered workflows. Used mainly for testing.
     */
    public static void clear() {
        REGISTRY.clear();
    }
}
