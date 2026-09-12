import { useState } from "react";
import { ChevronRight } from "lucide-react";
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
            <CollapsibleContent>
                <pre className="mt-2 max-h-64 overflow-auto rounded-md bg-destructive/5 p-2 text-xs whitespace-pre-wrap">
                    {details}
                </pre>
            </CollapsibleContent>
        </Collapsible>
    );
}
