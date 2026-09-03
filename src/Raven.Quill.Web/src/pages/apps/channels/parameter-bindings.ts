import { z } from "zod";
import type { AgentSummaryResponse, ChannelParameterBinding, ChannelParameterSource } from "@/api/generated/server-api";

export type ParameterSource = NonNullable<ChannelParameterSource>;

export type ParameterBindingRow = { name: string; source: ParameterSource; value: string };

export type ParameterBindingsFormData = { parameters: ParameterBindingRow[] };

export function parameterBindingsFormSchema(
    sourceValues: readonly [ParameterSource, ...ParameterSource[]],
): z.ZodType<ParameterBindingsFormData, ParameterBindingsFormData> {
    const rowSchema = z
        .object({
            name: z.string(),
            source: z.enum(sourceValues),
            value: z.string().trim(),
        })
        .superRefine((parameter, ctx) => {
            if (parameter.source === "Constant" && parameter.value.trim().length === 0) {
                ctx.addIssue({ code: "custom", message: "Required", path: ["value"] });
            }
        });

    return z.object({ parameters: z.array(rowSchema) }) as unknown as z.ZodType<
        ParameterBindingsFormData,
        ParameterBindingsFormData
    >;
}

export function toParameterBindings(parameters: readonly ParameterBindingRow[]) {
    const bindings: Record<string, ChannelParameterBinding> = {};
    for (const { name, source, value } of parameters) {
        bindings[name] = { source, value: source === "Constant" ? value.trim() : null };
    }
    return bindings;
}

export function seedParameterRows(agents: AgentSummaryResponse[], agentId: string) {
    const selected = agents.find((candidate) => candidate.agentId === agentId);
    return (selected?.parameters ?? []).map((parameter) => ({
        name: parameter.name,
        source: "Constant" as const,
        value: "",
    }));
}

// The rows to edit are the ones the routed agent declares; each is pre-filled from the channel's existing
// binding when it has one. Falls back to the stored binding keys if the agent can't be resolved, so a
// channel never hides bindings it already has. A binding whose source another channel type owns falls
// back to Constant.
export function seedEditRows(
    agent: AgentSummaryResponse | undefined,
    bindings: Record<string, ChannelParameterBinding> | null | undefined,
    sourceValues: readonly [ParameterSource, ...ParameterSource[]],
): ParameterBindingRow[] {
    const names = agent?.parameters?.length
        ? agent.parameters.map((parameter) => parameter.name)
        : Object.keys(bindings ?? {});
    return names.map((name) => {
        const binding = bindings?.[name];
        return {
            name,
            source: sourceValues.find((candidate) => candidate === binding?.source) ?? "Constant",
            value: binding?.source === "Constant" ? (binding.value ?? "") : "",
        };
    });
}

export function hasSameParameterNames(rows: readonly { name: string }[], seeded: readonly { name: string }[]) {
    return rows.length === seeded.length && seeded.every((row, index) => rows[index]?.name === row.name);
}
