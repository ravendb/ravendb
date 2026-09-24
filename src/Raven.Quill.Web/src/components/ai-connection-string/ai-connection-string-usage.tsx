import type { AiConnectionStringUsage, AiConnectionStringUsageKind } from "@/api/generated/server-api";
import { Badge } from "@/components/shadcn/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
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
    if (usedBy.length === 1) {
        return <p className={className}>{getUsageLabel(usedBy[0])}</p>;
    }

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

type AiConnectionStringUsageBadgeProps = {
    usedBy: AiConnectionStringUsage[];
};

export function AiConnectionStringUsageBadge({ usedBy }: AiConnectionStringUsageBadgeProps) {
    if (usedBy.length === 0) {
        return (
            <Badge variant="outline" className="text-muted-foreground">
                Not used
            </Badge>
        );
    }

    return (
        <Tooltip>
            <TooltipTrigger asChild>
                <Badge asChild variant="secondary">
                    <button type="button" className="cursor-default">
                        {usedBy.length === 1 ? "1 use" : `${usedBy.length} uses`}
                    </button>
                </Badge>
            </TooltipTrigger>
            <TooltipContent>
                <AiConnectionStringUsageList usedBy={usedBy} />
            </TooltipContent>
        </Tooltip>
    );
}
