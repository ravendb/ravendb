import type { AiAgentParameterPolicy, AiAgentParameterValueType } from "@/api/generated/server-api";
import { z } from "zod";

export const AGENT_PARAMETER_TYPES: AiAgentParameterValueType[] = [
    "Default",
    "String",
    "Number",
    "Boolean",
    "ArrayOfString",
    "ArrayOfNumber",
    "ArrayOfBoolean",
    "Null",
] as const;

export const AGENT_PARAMETER_POLICIES: AiAgentParameterPolicy[] = ["Default", "ForbidModelGeneration"] as const;

// Tri-state for the optional boolean query-tool options ("Default" lets the server decide).
export const QUERY_TOOL_OPTION_CHOICES = ["Default", "True", "False"] as const;

// How much of a webhook's response may be fed back to the model. The server defaults to 4 KB when the
// binding leaves it empty; these bounds are the form's own, since the API does not range-check the field.
export const DEFAULT_ACTION_RESPONSE_BYTES = 4 * 1024;
const MIN_ACTION_RESPONSE_BYTES = 256;
const MAX_ACTION_RESPONSE_BYTES = 64 * 1024;

const agentParameterSchema = z.object({
    name: z.string().trim().min(1, "Parameter name is required"),
    type: z.enum(AGENT_PARAMETER_TYPES),
    description: z.string(),
    policy: z.enum(AGENT_PARAMETER_POLICIES),
    isSendToModel: z.boolean(),
    // UI-only flag driving the expanded/collapsed card state; stripped before the API call.
    isExpanded: z.boolean(),
});

// Parameters stay optional: parameterless queries are valid, and the provision payload
// falls back to a "{}" sample object when both fields are empty (see agent-config-form.ts).
const agentQueryToolSchema = z.object({
    name: z
        .string()
        .trim()
        .min(1, "Tool name is required")
        .regex(/^[a-zA-Z0-9_-]+$/, "Tool name can only contain letters, numbers, underscores and hyphens"),
    description: z.string().trim().min(1, "Description is required"),
    query: z.string().trim().min(1, "Query is required"),
    parametersSampleObject: z.string(),
    parametersSchema: z.string(),
    allowModelQueries: z.enum(QUERY_TOOL_OPTION_CHOICES),
    addToInitialContext: z.enum(QUERY_TOOL_OPTION_CHOICES),
    isExpanded: z.boolean(),
});

// An action the LLM can trigger, together with the webhook Quill calls for it. The two are
// edited as one row so they cannot drift apart — the server rejects an action without a
// binding, and a binding without an action.
const agentActionSchema = z
    .object({
        name: z
            .string()
            .trim()
            .min(1, "Action name is required")
            .regex(/^[a-zA-Z0-9_-]+$/, "Action name can only contain letters, numbers, underscores and hyphens"),
        description: z.string().trim().min(1, "Description is required"),
        parametersSampleObject: z.string(),
        parametersSchema: z.string(),
        url: z
            .string()
            .trim()
            .min(1, "Webhook URL is required")
            .refine(isHttpUrl, "Webhook URL must be an absolute http:// or https:// address"),
        secret: z.string(),
        maxResponseSize: z
            .number()
            .int()
            .min(MIN_ACTION_RESPONSE_BYTES, `Cap must be at least ${MIN_ACTION_RESPONSE_BYTES} bytes`)
            .max(MAX_ACTION_RESPONSE_BYTES, `Cap must be at most ${MAX_ACTION_RESPONSE_BYTES} bytes`)
            .nullable(),
        isExpanded: z.boolean(),
    })
    .superRefine((action, ctx) => {
        // the server takes the schema and ignores the sample object when both are set, so it rejects the pair
        if (action.parametersSampleObject.trim() && action.parametersSchema.trim()) {
            ctx.addIssue({
                code: "custom",
                message: "Provide a sample parameters object or a schema, not both",
                path: ["parametersSchema"],
            });
        }
    });

function isHttpUrl(value: string) {
    try {
        const { protocol } = new URL(value);
        return protocol === "http:" || protocol === "https:";
    } catch {
        return false;
    }
}

// The working copy of the agent configuration edited in the review step. Seeded from an
// AI suggestion (mode "ai") or left empty (mode "manual"); always the source the wizard
// provisions from.
const agentConfigurationSchema = z
    .object({
        name: z.string().trim().min(1, "Agent name is required"),
        identifier: z.string(),
        systemPrompt: z.string().trim().min(1, "System prompt is required"),
        sampleObject: z.string(),
        outputSchema: z.string(),
        parameters: z.array(agentParameterSchema),
        queries: z.array(agentQueryToolSchema),
        actions: z.array(agentActionSchema),
    })
    .superRefine((config, ctx) => {
        addDuplicateNameIssues(ctx, config.parameters, "parameters", "Parameter name must be unique");
        addDuplicateNameIssues(ctx, config.queries, "queries", "Tool name must be unique");
        addDuplicateNameIssues(ctx, config.actions, "actions", "Action name must be unique");
        addToolNameCollisionIssues(ctx, config.queries, config.actions);
        if (config.sampleObject.trim().length === 0 && config.outputSchema.trim().length === 0) {
            ctx.addIssue({
                code: "custom",
                message: "Either 'Sample response object' or 'Response JSON schema' must be provided",
                path: ["outputSchema"],
            });
        }
    });

// Queries and actions share one tool-name namespace on the server, so a name used by both
// is rejected there; flag it on the action instead of shipping the operator a 400.
function addToolNameCollisionIssues(ctx: z.RefinementCtx, queries: { name: string }[], actions: { name: string }[]) {
    const queryNames = new Set(queries.map((query) => query.name));

    actions.forEach((action, index) => {
        if (action.name && queryNames.has(action.name)) {
            ctx.addIssue({
                code: "custom",
                message: "A query tool already uses this name",
                path: ["actions", index, "name"],
            });
        }
    });
}

function addDuplicateNameIssues(ctx: z.RefinementCtx, items: { name: string }[], listKey: string, message: string) {
    const counts = new Map<string, number>();

    for (const item of items) {
        counts.set(item.name, (counts.get(item.name) ?? 0) + 1);
    }

    items.forEach((item, index) => {
        if (item.name && (counts.get(item.name) ?? 0) > 1) {
            ctx.addIssue({ code: "custom", message, path: [listKey, index, "name"] });
        }
    });
}

// The wizard currently supports a single capability (AI Agent). The literal keeps the
// "Choose an AI Capability" step honest while leaving room for more capabilities later.
export const agentSchema = z.object({
    capability: z.object({
        type: z.literal("agent"),
    }),
    connection: z.object({
        connectionStringName: z.string().min(1, "Select an AI provider connection string"),
    }),
    create: z
        .object({
            // "ai": start from an AI-suggested data candidate; "prompt": generate one from a
            // free-text description; "manual": build the configuration from scratch.
            mode: z.enum(["ai", "prompt", "manual"]),
            // Index into the AI-suggested candidates held in the wizard store.
            selectedIndex: z.number().int().min(0),
            // Free-text intent that drives the "from-prompt" suggest mode. UI-only: it is the
            // input to generation, not part of the provisioned configuration.
            promptInput: z.string(),
        })
        .superRefine((value, ctx) => {
            if (value.mode === "prompt" && value.promptInput.trim().length === 0) {
                ctx.addIssue({
                    code: "custom",
                    message: "Describe what you'd like your agent to do",
                    path: ["promptInput"],
                });
            }
        }),
    review: agentConfigurationSchema,
});

export type AgentFormData = z.infer<typeof agentSchema>;
export type AgentConfigurationFormData = AgentFormData["review"];
export type AgentParameterFormData = AgentConfigurationFormData["parameters"][number];
export type AgentQueryToolFormData = AgentConfigurationFormData["queries"][number];
export type AgentActionFormData = AgentConfigurationFormData["actions"][number];

// "channels" has no form fields of its own (channels are created through their own API);
// it is an optional step shown after the agent is provisioned.
export type AgentStepId = keyof AgentFormData | "channels";
