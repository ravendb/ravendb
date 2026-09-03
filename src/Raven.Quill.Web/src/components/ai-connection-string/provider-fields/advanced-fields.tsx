import { type ReactNode, useState } from "react";
import { ChevronDown } from "lucide-react";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";

// Tucks a provider's optional overrides and tuning knobs behind a collapsible section so the
// common case (credentials + model) stays front and center. Opens by default when editing a
// connection string that already has advanced values set.
export function AdvancedFields({ defaultOpen = false, children }: { defaultOpen?: boolean; children: ReactNode }) {
    const [isOpen, setIsOpen] = useState(defaultOpen);

    return (
        <Collapsible open={isOpen} onOpenChange={setIsOpen} className="grid gap-4 rounded-md border bg-card p-2">
            <CollapsibleTrigger className="group flex w-full items-center justify-between gap-2 text-left text-sm font-medium">
                Advanced
                <ChevronDown
                    className="size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180"
                    aria-hidden="true"
                />
            </CollapsibleTrigger>
            <CollapsibleContent className="grid gap-4">{children}</CollapsibleContent>
        </Collapsible>
    );
}
