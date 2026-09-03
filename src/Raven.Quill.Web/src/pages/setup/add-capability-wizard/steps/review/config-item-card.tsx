import type { ReactNode } from "react";
import { ChevronDown, ChevronUp, Trash2 } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Separator } from "@/components/shadcn/ui/separator";
import { Text } from "@/components/typography";

type ConfigItemCardProps = {
    isExpanded: boolean;
    // Title of the expanded editor, e.g. "Configure parameter".
    editTitle: string;
    // Collapsed one-line summary of the item.
    summary: ReactNode;
    onToggleExpanded: (isExpanded: boolean) => void;
    onRemove: () => void;
    children: ReactNode;
};

// Collapsed/expanded shell shared by the parameter and query-tool list items in the
// agent configuration editor (same interaction as Studio's EditAiAgent items).
export function ConfigItemCard({
    isExpanded,
    editTitle,
    summary,
    onToggleExpanded,
    onRemove,
    children,
}: ConfigItemCardProps) {
    const actions = (
        <div className="flex shrink-0 items-center gap-1.5">
            <Button variant="destructive" size="icon-sm" aria-label="Remove" onClick={onRemove}>
                <Trash2 />
            </Button>
            <Button
                variant="ghost"
                size="icon-sm"
                aria-label={isExpanded ? "Collapse" : "Edit"}
                onClick={() => onToggleExpanded(!isExpanded)}
            >
                {isExpanded ? <ChevronUp /> : <ChevronDown />}
            </Button>
        </div>
    );

    if (!isExpanded) {
        return (
            <div className="flex items-center justify-between gap-3 rounded-md border bg-background px-3 py-2">
                <div className="min-w-0 flex-1">{summary}</div>
                {actions}
            </div>
        );
    }

    return (
        <div className="grid gap-3 rounded-md border bg-background p-3">
            <div className="flex items-center justify-between gap-3">
                <span className="text-sm font-semibold">{editTitle}</span>
                {actions}
            </div>
            <Separator />
            {children}
        </div>
    );
}

export function ConfigListEmpty({ label }: { label: string }) {
    return (
        <Text
            variant="caption"
            as="div"
            className="flex items-center justify-center rounded-md border border-dashed py-6"
        >
            {label}
        </Text>
    );
}
