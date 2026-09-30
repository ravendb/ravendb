import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export interface ParameterItem {
    name: string;
    value: string;
}

// Renders a list of bound key/value parameters as compact monospace chips. Shared across the
// conversations table and the channel embed-link table so parameter styling stays consistent.
export function Parameters({
    params,
    className,
    emptyFallback = "—",
}: {
    params: ParameterItem[];
    className?: string;
    emptyFallback?: ReactNode;
}) {
    if (params.length === 0) {
        return <span className="text-muted-foreground">{emptyFallback}</span>;
    }

    const items = params.map((param) => ({ name: param.name, value: formatParameterValue(param.value) }));

    return (
        <span
            className={cn("flex items-center gap-1.5", className)}
            title={items.map((item) => `${item.name}=${item.value}`).join("  ")}
        >
            {items.map((item) => (
                <Parameter key={item.name} name={item.name} value={item.value} />
            ))}
        </span>
    );
}

function Parameter({ name, value }: ParameterItem) {
    return (
        <span className="inline-flex shrink-0 items-center rounded-md border bg-muted/40 px-1.5 py-0.5 font-mono text-xs leading-none">
            <span className="text-muted-foreground">{name}</span>
            <span className="px-0.5 text-muted-foreground/50">=</span>
            <span className="font-medium text-foreground">{value}</span>
        </span>
    );
}

// The backend serializes a parameter binding ({ "Value": ..., "SendToModel": ... }) into the value
// field, so surface just the bound value. Plain scalar values are returned unchanged.
function formatParameterValue(raw: string): string {
    const trimmed = typeof raw === "string" ? raw.trim() : "";
    if (!trimmed.startsWith("{")) {
        return typeof raw === "string" ? raw : String(raw);
    }
    return parseBoundValue(trimmed) ?? raw;
}

function parseBoundValue(json: string): string | null {
    try {
        const parsed = JSON.parse(json) as { Value?: unknown; value?: unknown };
        const value = parsed.Value ?? parsed.value;
        if (value === undefined || value === null) {
            return null;
        }
        return typeof value === "string" ? value : JSON.stringify(value);
    } catch {
        return null;
    }
}
