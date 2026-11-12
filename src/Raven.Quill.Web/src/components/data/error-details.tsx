import { useState } from "react";
import { ChevronRight } from "lucide-react";
import { CopyableCode } from "@/components/data/copyable-code";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { cn } from "@/lib/utils";

export function ErrorDetails({ details }: { details: string }) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <Collapsible open={isOpen} onOpenChange={setIsOpen}>
            <CollapsibleTrigger className="flex items-center gap-1 text-xs font-medium underline-offset-2 hover:underline">
                <ChevronRight
                    className={cn("size-3.5 transition-transform", isOpen && "rotate-90")}
                    aria-hidden="true"
                />
                {isOpen ? "Hide details" : "Show details"}
            </CollapsibleTrigger>
            <CollapsibleContent className="mt-2">
                <CopyableCode
                    code={details}
                    copyLabel="Copy the error details"
                    className="max-h-64 overflow-y-auto rounded-md border-0 bg-destructive/5"
                />
            </CollapsibleContent>
        </Collapsible>
    );
}
