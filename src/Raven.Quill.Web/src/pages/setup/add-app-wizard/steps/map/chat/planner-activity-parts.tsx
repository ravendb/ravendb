import { type ReactNode, useState } from "react";
import { Check, ChevronDown, ChevronRight, TriangleAlert, X } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Text } from "@/components/typography";
import { cn } from "@/lib/utils";
import { usePlannerActions } from "@/pages/setup/add-app-wizard/steps/map/use-planner-actions";

export function PlannerWorkingActivity({
    title,
    detail,
    children,
}: {
    title: string;
    detail?: string;
    children: ReactNode;
}) {
    const [isOpen, setIsOpen] = useState(true);

    return (
        <ActivityGroup
            icon={<Spinner className="size-3.5 text-primary-strong" aria-hidden="true" />}
            title={title}
            detail={detail}
            isOpen={isOpen}
            onOpenChange={setIsOpen}
        >
            {children}
        </ActivityGroup>
    );
}

export function ActivityGroup({
    icon,
    title,
    detail,
    isDetailDestructive = false,
    isOpen,
    onOpenChange,
    children,
}: {
    icon: ReactNode;
    title: string;
    detail?: string;
    isDetailDestructive?: boolean;
    isOpen: boolean;
    onOpenChange: (isOpen: boolean) => void;
    children: ReactNode;
}) {
    const Chevron = isOpen ? ChevronDown : ChevronRight;

    return (
        <Collapsible open={isOpen} onOpenChange={onOpenChange}>
            <div className="flex items-center gap-2">
                <div role="status" className="flex items-center gap-2">
                    <span className="flex size-3.5 shrink-0 items-center justify-center">{icon}</span>
                    <Text variant="label" as="span">
                        {title}
                    </Text>
                    {detail && (
                        <Text
                            variant="muted"
                            as="span"
                            className={cn(isDetailDestructive && "text-badge-destructive-fg")}
                        >
                            {detail}
                        </Text>
                    )}
                </div>
                <CollapsibleTrigger asChild>
                    <Button variant="ghost" size="icon-xs" aria-label="Steps" className="text-muted-foreground">
                        <Chevron aria-hidden="true" />
                    </Button>
                </CollapsibleTrigger>
            </div>
            <CollapsibleContent>
                <ul className="mt-1 ml-1.5 flex flex-col gap-1.5 border-l py-1 pl-4">{children}</ul>
            </CollapsibleContent>
        </Collapsible>
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
    const { openCollectionByName } = usePlannerActions();

    return (
        <li className={cn("flex flex-wrap items-center gap-2", className)}>
            {icon}
            {collection && (
                <Button
                    variant="link"
                    className="h-auto p-0 text-foreground underline decoration-muted-foreground/60 underline-offset-3"
                    onClick={() => openCollectionByName(collection)}
                >
                    {collection}
                </Button>
            )}
            <Text variant="muted" as="span" className="min-w-0 flex-1">
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
