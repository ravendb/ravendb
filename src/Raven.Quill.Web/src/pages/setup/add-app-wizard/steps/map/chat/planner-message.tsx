import type { ReactNode } from "react";
import { Layers, Sparkle } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Text } from "@/components/typography";

export function PlannerMessage({ children }: { children: ReactNode }) {
    return (
        <div className="flex items-start gap-3">
            <span
                aria-hidden="true"
                className="mt-px flex size-6 shrink-0 items-center justify-center rounded-full bg-badge-default-bg text-primary-strong"
            >
                <Sparkle className="size-3.5" />
            </span>
            <div className="flex min-w-0 flex-1 flex-col gap-2.5">{children}</div>
        </div>
    );
}

export function PlannerPlanCard({
    title,
    detail,
    actionLabel,
    onAction,
}: {
    title: string;
    detail: string;
    actionLabel: string;
    onAction: () => void;
}) {
    return (
        <div className="flex items-center gap-3 rounded-lg border bg-card py-2.5 pr-2.5 pl-3">
            <span aria-hidden="true" className="flex size-9 shrink-0 items-center justify-center rounded-md bg-muted">
                <Layers className="size-4" />
            </span>
            <div className="flex min-w-0 flex-1 flex-col">
                <Text as="span" variant="label">
                    {title}
                </Text>
                <Text variant="muted" as="span">
                    {detail}
                </Text>
            </div>
            <Button variant="outline" size="sm" onClick={onAction}>
                {actionLabel}
            </Button>
        </div>
    );
}
