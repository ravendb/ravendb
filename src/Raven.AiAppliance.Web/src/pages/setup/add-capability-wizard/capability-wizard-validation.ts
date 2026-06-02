import { z } from "zod";

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
        // Index into the AI-suggested candidates held in the wizard store.
        selectedIndex: z.number().int().min(0),
        systemPrompt: z.string().trim().min(1, "System prompt is required"),
    }),
    review: z.object({
        name: z.string().trim().min(1, "Agent name is required"),
    }),
});

export type AgentFormData = z.infer<typeof agentSchema>;

// "channels" has no form fields of its own (it is the final save step), so it is not a
// schema key — but it is still a wizard step the stepper renders.
export type AgentStepId = keyof AgentFormData | "channels";
