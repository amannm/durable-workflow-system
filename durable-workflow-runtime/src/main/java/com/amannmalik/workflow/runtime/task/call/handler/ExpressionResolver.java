package com.amannmalik.workflow.runtime.task.call.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ExpressionResolver {
  private static final Logger log = LoggerFactory.getLogger(ExpressionResolver.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ExpressionResolver() {}

  static String resolveExpressions(WorkflowContext ctx, String value) {
    if (value == null) {
      return null;
    }
    value = substitute(ctx, value, Pattern.compile("\\$\\{([^}]+)}"));
    value = substitute(ctx, value, Pattern.compile("\\{([^}]+)}"));
    return value;
  }

  private static String substitute(WorkflowContext ctx, String value, Pattern pattern) {
    Matcher m = pattern.matcher(value);
    StringBuilder sb = new StringBuilder();
    while (m.find()) {
      String expr = m.group(1).trim();
      String replacement = evaluateJq(ctx, expr);
      m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  private static String evaluateJq(WorkflowContext ctx, String expr) {
    expr = expr.trim();
    if (!expr.startsWith(".")) {
      expr = "." + expr;
    }
    ObjectNode root = MAPPER.createObjectNode();
    java.util.Set<String> vars = new java.util.HashSet<>();
    java.util.regex.Matcher varMatcher = Pattern.compile("\\.([a-zA-Z0-9_]+)").matcher(expr);
    while (varMatcher.find()) {
      vars.add(varMatcher.group(1));
    }
    for (String key : vars) {
      ctx.get(StateKey.of(key, Object.class)).ifPresent(v -> root.set(key, MAPPER.valueToTree(v)));
    }
    try {
      JsonQuery q = JsonQuery.compile(expr, Versions.JQ_1_6);
      List<com.fasterxml.jackson.databind.JsonNode> out = new java.util.ArrayList<>();
      q.apply(Scope.newEmptyScope(), root, out::add);
      if (out.isEmpty()) {
        return "";
      }
      com.fasterxml.jackson.databind.JsonNode r = out.getLast();
      return r.isTextual() ? r.asText() : r.toString();
    } catch (Exception e) {
      log.error("Failed to evaluate expression: {}", expr, e);
      return "";
    }
  }
}
