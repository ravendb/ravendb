import { CircleSlash2Icon, PlusIcon } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { cn } from "@/lib/utils";

export function DiscoverLoadingSkeleton() {
    return (
        <div className="grid gap-2">
            {Array.from({ length: 6 }).map((_, index) => (
                <Skeleton key={index} className="h-10 w-full" />
            ))}
        </div>
    );
}

export function NoTablesFound({ schemas, onCustomizeSchemas }: { schemas: string[]; onCustomizeSchemas: () => void }) {
    const scopeLabel =
        schemas.length === 0
            ? "the default schema"
            : schemas.length === 1
              ? `the "${schemas[0]}" schema`
              : `schemas: ${schemas.join(", ")}`;

    return (
        <div className="flex flex-col items-center justify-center gap-4 rounded-lg border py-16">
            <div className="rounded-lg bg-secondary p-2.5">
                <CircleSlash2Icon className="size-5 text-muted-foreground" aria-hidden="true" />
            </div>
            <div className="text-center">
                <p className="text-sm font-medium">We didn&apos;t find any tables in {scopeLabel}</p>
                <p className="mt-1 text-xs text-muted-foreground">
                    Add other schemas to widen the table discovery scope.
                </p>
            </div>
            <Button type="button" variant="outline" size="sm" onClick={onCustomizeSchemas}>
                <PlusIcon aria-hidden="true" />
                Customize schemas
            </Button>
        </div>
    );
}

export function MessageList({
    messages,
    tone = "muted",
}: {
    messages?: string[];
    tone?: "destructive" | "muted" | "warning";
}) {
    const visibleMessages = messages?.filter(Boolean) ?? [];

    if (visibleMessages.length === 0) {
        return null;
    }

    return (
        <ul
            className={cn(
                "grid gap-1 text-sm",
                tone === "destructive" && "text-destructive",
                tone === "warning" && "text-amber-700 dark:text-amber-300",
                tone === "muted" && "text-muted-foreground",
            )}
        >
            {visibleMessages.map((message, index) => (
                <li key={index}>{message}</li>
            ))}
        </ul>
    );
}
