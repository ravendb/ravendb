import type {
    AgentSummaryResponse,
    TelegramParameterBinding,
    TelegramParameterSource,
} from "@/api/generated/server-api";

export type ParameterSource = NonNullable<TelegramParameterSource>;

export type ParameterBindingRow = { name: string; source: ParameterSource; value: string };

export function toParameterBindings(parameters: readonly ParameterBindingRow[]) {
    const bindings: Record<string, TelegramParameterBinding> = {};
    for (const { name, source, value } of parameters) {
        bindings[name] = { source, value: source === "Constant" ? value.trim() : null };
    }
    return bindings;
}

export function seedParameterRows(agents: AgentSummaryResponse[], agentId: string) {
    const selected = agents.find((candidate) => candidate.agentId === agentId);
    return (selected?.parameters ?? []).map((name) => ({ name, source: "Constant" as const, value: "" }));
}

export function hasSameParameterNames(rows: readonly { name: string }[], seeded: readonly { name: string }[]) {
    return rows.length === seeded.length && seeded.every((row, index) => rows[index]?.name === row.name);
}
