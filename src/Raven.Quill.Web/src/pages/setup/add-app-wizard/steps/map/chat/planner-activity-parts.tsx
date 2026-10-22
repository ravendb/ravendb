import type { ReactNode } from "react";
import { Check, ChevronDown, ChevronRight, TriangleAlert, X } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Text } from "@/components/typography";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";

export function PlannerWorkingCard({
    title,
    detail,
    children,
}: {
    title: string;
    detail?: string;
    children: ReactNode;
}) {
    return (
        <div className="rounded-lg border bg-card">
            <div role="status" className="flex items-center gap-2 px-3 py-2">
                <Spinner className="text-primary-strong" aria-hidden="true" />
                <Text variant="label" as="span">
                    {title}
                </Text>
                {detail && (
                    <Text variant="muted" as="span" className="ml-auto">
                        {detail}
                    </Text>
                )}
            </div>
            <ActivityRowList>{children}</ActivityRowList>
        </div>
    );
}

export function ActivityRowList({ children }: { children: ReactNode }) {
    return <ul className="flex flex-col gap-1.5 pr-3 pb-2.5 pl-9">{children}</ul>;
}

export function CollapsibleHeader({
    isOpen,
    className,
    children,
}: {
    isOpen: boolean;
    className?: string;
    children: ReactNode;
}) {
    const Chevron = isOpen ? ChevronDown : ChevronRight;

    return (
        <CollapsibleTrigger className={cn("flex w-full items-center gap-2 py-2 text-left", className)}>
            <Chevron className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
            {children}
        </CollapsibleTrigger>
    );
}

export function ActivityRowLayout({
    icon,
    collection,
    detail,
    className,
    children,
}: {
    icon: ReactNode;
    collection?: string;
    detail?: string | null;
    className?: string;
    children: ReactNode;
}) {
    const openPlannerPanel = useSetupWizardStore((state) => state.openPlannerPanel);

    return (
        <li className={cn("flex flex-wrap items-center gap-2", className)}>
            {icon}
            {collection && (
                <Button
                    variant="link"
                    className="h-auto p-0 text-foreground underline decoration-border underline-offset-3"
                    onClick={() => openPlannerPanel(collection)}
                >
                    {collection}
                </Button>
            )}
            <Text variant="muted" as="span">
                {children}
            </Text>
            {detail && (
                <Text variant="caption" className="basis-full pl-5">
                    {detail}
                </Text>
            )}
        </li>
    );
}

export function DoneIcon() {
    return <Check className="size-3 shrink-0 text-success" aria-hidden="true" />;
}

export function WorkingIcon() {
    return <Spinner className="size-3 text-muted-foreground" aria-hidden="true" />;
}

export function PendingIcon() {
    return <span aria-hidden="true" className="size-3 shrink-0 rounded-full border border-input" />;
}

export function WarningIcon() {
    return <TriangleAlert className="size-3 shrink-0 text-badge-warning-fg" aria-hidden="true" />;
}

export function FailedIcon() {
    return <X className="size-3 shrink-0 text-destructive" aria-hidden="true" />;
}
