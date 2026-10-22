import { Layers, X } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Text } from "@/components/typography";
import { cn } from "@/lib/utils";

export function PlannerMentionChip({ collection, onRemove }: { collection: string; onRemove?: () => void }) {
    return (
        <Text
            as="span"
            variant="label"
            className={cn(
                "inline-flex h-6.5 items-center gap-1.5 rounded-md border bg-muted pl-2",
                onRemove ? "pr-1" : "pr-2",
            )}
        >
            <Layers className="size-3 shrink-0" aria-hidden="true" />
            {collection}
            {onRemove && (
                <Button
                    variant="ghost"
                    size="icon-xs"
                    className="size-4.5 text-muted-foreground hover:bg-background"
                    aria-label={`Remove ${collection} from the prompt`}
                    onClick={onRemove}
                >
                    <X className="size-3" aria-hidden="true" />
                </Button>
            )}
        </Text>
    );
}
