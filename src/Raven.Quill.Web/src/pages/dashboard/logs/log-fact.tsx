import type { PropsWithChildren } from "react";

/** One read-only fact about a log sink, matching the key-value rows the other settings pages use. */
export function LogFact({ label, children }: PropsWithChildren<{ label: string }>) {
    return (
        <div className="space-y-1">
            <div className="text-xs text-muted-foreground">{label}</div>
            <div className="text-sm font-medium break-all">{children}</div>
        </div>
    );
}
