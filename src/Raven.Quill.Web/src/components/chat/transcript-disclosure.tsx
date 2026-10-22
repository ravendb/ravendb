import { createContext, useContext, useState, type ReactNode } from "react";
import { ChevronDown, type LucideIcon } from "lucide-react";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { cn } from "@/lib/utils";

type OpenDisclosures = {
    isOpen: (key: string) => boolean;
    setOpen: (key: string, isOpen: boolean) => void;
};

const OpenDisclosuresContext = createContext<OpenDisclosures | null>(null);

// Transcript rows are virtualized, so a row unmounts as soon as it scrolls out of view. Keeping what
// the operator expanded here survives that; inside the disclosure it would silently collapse.
export function TranscriptDisclosureState({ children }: { children: ReactNode }) {
    const [openKeys, setOpenKeys] = useState<ReadonlySet<string>>(new Set());

    const openDisclosures: OpenDisclosures = {
        isOpen: (key) => openKeys.has(key),
        setOpen: (key, isOpen) =>
            setOpenKeys((keys) => {
                const next = new Set(keys);
                if (isOpen) {
                    next.add(key);
                } else {
                    next.delete(key);
                }
                return next;
            }),
    };

    return <OpenDisclosuresContext.Provider value={openDisclosures}>{children}</OpenDisclosuresContext.Provider>;
}

export function TranscriptDisclosure({
    disclosureKey,
    icon: Icon,
    label,
    children,
}: {
    disclosureKey: string;
    icon: LucideIcon;
    label: string;
    children: ReactNode;
}) {
    const openDisclosures = useContext(OpenDisclosuresContext);
    if (openDisclosures === null) {
        throw new Error("TranscriptDisclosure must be rendered inside TranscriptDisclosureState");
    }

    return (
        <Collapsible
            open={openDisclosures.isOpen(disclosureKey)}
            onOpenChange={(isOpen) => openDisclosures.setOpen(disclosureKey, isOpen)}
            className="w-full overflow-hidden rounded-lg border bg-muted/40 text-sm"
        >
            <CollapsibleTrigger className="group flex w-full items-center justify-between gap-2 px-3 py-2 text-left transition-colors hover:bg-muted/70">
                <span className="flex min-w-0 items-center gap-2">
                    <span className="flex size-6 shrink-0 items-center justify-center rounded-md bg-primary/10 text-primary-strong">
                        <Icon className="size-3.5" aria-hidden />
                    </span>
                    <span className="truncate font-medium">{label}</span>
                </span>
                <ChevronDown
                    className="size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180"
                    aria-hidden
                />
            </CollapsibleTrigger>
            <CollapsibleContent>{children}</CollapsibleContent>
        </Collapsible>
    );
}

export function CodeBlock({ value, className }: { value: string; className?: string }) {
    return (
        <pre
            className={cn(
                "max-h-60 overflow-auto rounded-md border bg-background p-2 font-mono text-xs whitespace-pre-wrap",
                className,
            )}
        >
            {value}
        </pre>
    );
}
