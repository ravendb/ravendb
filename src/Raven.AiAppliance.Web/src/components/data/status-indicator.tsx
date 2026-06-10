import { cn } from "@/lib/utils";

export function StatusIndicator({ tone, label }: { tone: "positive" | "muted"; label: string }) {
    return (
        <span
            className={cn(
                "inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium",
                tone === "positive"
                    ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400"
                    : "bg-muted text-muted-foreground",
            )}
        >
            <span
                className={cn(
                    "size-1.5 rounded-full",
                    tone === "positive" ? "bg-emerald-500" : "bg-muted-foreground/50",
                )}
                aria-hidden="true"
            />
            {label}
        </span>
    );
}
