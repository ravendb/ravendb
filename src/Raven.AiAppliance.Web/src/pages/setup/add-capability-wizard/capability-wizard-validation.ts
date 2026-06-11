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

const agentParameterSchema = z.object({
    name: z.string().trim().min(1, "Parameter name is required"),
    type: z.enum(AGENT_PARAMETER_TYPES),
    description: z.string(),
    policy: z.enum(AGENT_PARAMETER_POLICIES),
    isSendToModel: z.boolean(),
    // UI-only flag driving the expanded/collapsed card state; stripped before the API call.
    isExpanded: z.boolean(),
});

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
    })
    .superRefine((config, ctx) => {
        addDuplicateNameIssues(ctx, config.parameters, "parameters", "Parameter name must be unique");
        addDuplicateNameIssues(ctx, config.queries, "queries", "Tool name must be unique");
    });

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
    create: z.object({
        // "ai": start from an AI-suggested candidate; "manual": build the configuration from scratch.
        mode: z.enum(["ai", "manual"]),
        // Index into the AI-suggested candidates held in the wizard store.
        selectedIndex: z.number().int().min(0),
    }),
    review: agentConfigurationSchema,
});

export type AgentFormData = z.infer<typeof agentSchema>;
export type AgentConfigurationFormData = AgentFormData["review"];
export type AgentParameterFormData = AgentConfigurationFormData["parameters"][number];
export type AgentQueryToolFormData = AgentConfigurationFormData["queries"][number];

export type AgentStepId = keyof AgentFormData;
