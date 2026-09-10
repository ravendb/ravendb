import type {
    AgentSummaryResponse,
    AiAgentParameterPolicy,
    AiAgentParameterValueType,
} from "@/api/generated/server-api";
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

export type ExistingAgent = Pick<AgentSummaryResponse, "agentId" | "name">;

// Mirrors AiTaskIdentifierHelper.ValidateIdentifier, minus its blind spot: the server accepts a
// leading hyphen, which reads as a typo everywhere the identifier is shown.
const AGENT_IDENTIFIER_PATTERN = /^[a-z0-9]+(-[a-z0-9]+)*$/;

const FALLBACK_AGENT_IDENTIFIER = "agent";

const agentConfigurationFields = {
    name: z.string().trim().min(1, "Agent name is required"),
    systemPrompt: z.string().trim().min(1, "System prompt is required"),
    sampleObject: z.string(),
    outputSchema: z.string(),
    parameters: z.array(agentParameterSchema),
    queries: z.array(agentQueryToolSchema),
    actions: z.array(agentActionSchema),
};

/**
 * The working copy of the agent configuration edited in the review step. Seeded from an
 * AI suggestion (mode "ai") or left empty (mode "manual"); always the source the wizard
 * provisions from.
 *
 * `existingAgents` are the agents the app already has. The server updates an agent in place when
 * both its identifier and name match, and rejects an identifier clash with a 400. A duplicate name
 * alone it would accept, but two agents with one name are indistinguishable everywhere the app
 * lists them, so it is rejected here as well.
 */
export const createAgentConfigurationSchema = (existingAgents: ExistingAgent[] = []) =>
    z
        .object({
            ...agentConfigurationFields,
            identifier: z
                .string()
                .trim()
                .min(1, { error: "Identifier is required", abort: true })
                .regex(AGENT_IDENTIFIER_PATTERN, "Use lowercase letters (a-z), digits (0-9) and single hyphens"),
        })
        .superRefine((config, ctx) => {
            for (const conflict of findExistingAgentConflicts(config, existingAgents)) {
                ctx.addIssue({ code: "custom", message: conflict.message, path: [conflict.field] });
            }

            addAgentConfigurationIssues(config, ctx);
        });

// An agent's identifier is permanent
export const editAgentConfigurationSchema = z
    .object({ ...agentConfigurationFields, identifier: z.string() })
    .superRefine(addAgentConfigurationIssues);

// Mirrors AiTaskIdentifierHelper.GenerateIdentifier
export function toAgentIdentifier(name: string) {
    const identifier = name
        .normalize("NFD")
        .replace(/[^A-Za-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "")
        .toLowerCase();

    return identifier || FALLBACK_AGENT_IDENTIFIER;
}

type AgentConfigurationIssueInput = {
    sampleObject: string;
    outputSchema: string;
    parameters: { name: string }[];
    queries: { name: string }[];
    actions: { name: string }[];
};

function addAgentConfigurationIssues(config: AgentConfigurationIssueInput, ctx: z.RefinementCtx) {
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
}

export type ExistingAgentConflict = { field: "name" | "identifier"; message: string };

export function findExistingAgentConflicts(
    config: { name: string; identifier: string },
    existingAgents: ExistingAgent[],
): ExistingAgentConflict[] {
    const conflicts: ExistingAgentConflict[] = [];
    const takenName = findExisting(existingAgents, (agent) => agent.name, config.name);

    if (takenName) {
        conflicts.push({ field: "name", message: `This app already has an agent named "${takenName}"` });
    }

    const takenIdentifier = findExisting(existingAgents, (agent) => agent.agentId, config.identifier);

    if (takenIdentifier) {
        conflicts.push({
            field: "identifier",
            message: `Another agent in this app already uses the identifier "${takenIdentifier}"`,
        });
    }

    return conflicts;
}

function findExisting(agents: ExistingAgent[], select: (agent: ExistingAgent) => string, candidate: string) {
    const key = candidate.trim().toLowerCase();

    return key
        ? agents
              .map(select)
              .find((value) => value.trim().toLowerCase() === key)
              ?.trim()
        : undefined;
}

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

const connectionSchema = z.object({
    connectionStringName: z.string().min(1, "Select an AI provider connection string"),
});

// The wizard currently supports a single capability (AI Agent). The literal keeps the
// "Choose an AI Capability" step honest while leaving room for more capabilities later.
export const createAgentSchema = (existingAgents: ExistingAgent[] = []) =>
    z.object({
        capability: z.object({
            type: z.literal("agent"),
        }),
        connection: connectionSchema,
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
        review: createAgentConfigurationSchema(existingAgents),
    });

// The name of an existing agent is permanent and the edit form keeps it disabled, so editing needs
// no uniqueness check and no agent list to check against.
export const editAgentSchema = z.object({
    connection: connectionSchema,
    review: editAgentConfigurationSchema,
});

export type AgentFormData = z.infer<ReturnType<typeof createAgentSchema>>;
export type AgentConfigurationFormData = AgentFormData["review"];
export type AgentParameterFormData = AgentConfigurationFormData["parameters"][number];
export type AgentQueryToolFormData = AgentConfigurationFormData["queries"][number];
export type AgentActionFormData = AgentConfigurationFormData["actions"][number];

// "channels" has no form fields of its own (channels are created through their own API);
// it is an optional step shown after the agent is provisioned.
export type AgentStepId = keyof AgentFormData | "channels";
