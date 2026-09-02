import type { AiConnectionStringUsage, AiConnectionStringUsageKind } from "@/api/generated/server-api";
import { cn } from "@/lib/utils";

const USAGE_KIND_LABELS: Record<AiConnectionStringUsageKind, string> = {
    AiAgent: "Agent",
    GenAi: "GenAI task",
    EmbeddingsGeneration: "Embeddings task",
};

// A usage's database is the app's database, so it reads as the app it belongs to.
function getUsageLabel(usage: AiConnectionStringUsage) {
    const kind = USAGE_KIND_LABELS[usage.kind];
    const subject = usage.name ?? usage.identifier;
    const label = subject ? `${kind} “${subject}”` : kind;
    return usage.databaseName ? `${label} in app “${usage.databaseName}”` : label;
}

type AiConnectionStringUsageListProps = {
    usedBy: AiConnectionStringUsage[];
    className?: string;
};

export function AiConnectionStringUsageList({ usedBy, className }: AiConnectionStringUsageListProps) {
    return (
        <ul className={cn("list-disc pl-4", className)}>
            {usedBy.map((usage) => (
                <li key={`${usage.kind}:${usage.databaseName}:${usage.identifier ?? usage.name}`}>
                    {getUsageLabel(usage)}
                </li>
            ))}
        </ul>
    );
}
