import { ChevronDown, RotateCcw } from "lucide-react";
import { useRef, useState, type ReactNode } from "react";
import { useFormState, type Control } from "react-hook-form";
import { FormErrorIcon } from "@/components/form/form-error-icon";
import { Button } from "@/components/shadcn/ui/button";
import { Heading } from "@/components/typography";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import type { WidgetThemeFormData } from "@/pages/apps/channels/web-widget-theme-schema";

type ThemeEditorSectionProps = {
    title: string;
    control: Control<WidgetThemeFormData>;
    paths: readonly (keyof WidgetThemeFormData)[];
    /** Called with this section's paths so the caller can restore them from the saved theme. */
    onReset: (paths: readonly (keyof WidgetThemeFormData)[]) => void;
    defaultOpen?: boolean;
    children: ReactNode;
};

export function ThemeEditorSection({
    title,
    control,
    paths,
    onReset,
    defaultOpen = false,
    children,
}: ThemeEditorSectionProps) {
    const [isOpen, setIsOpen] = useState(defaultOpen);
    const { dirtyFields } = useFormState({ control });
    // Only offer the undo once there is something to undo, so a untouched section stays quiet.
    const isSectionDirty = paths.some((path) => path in dirtyFields);
    const triggerRef = useRef<HTMLButtonElement>(null);

    const onResetClick = () => {
        onReset(paths);
        // The reset button only renders while the section is dirty, so this click unmounts it -
        // without moving focus deliberately, the browser drops it to <body>, which for a keyboard or
        // screen-reader user reads as being bounced to the top of the document with no announcement.
        triggerRef.current?.focus();
    };

    return (
        <Collapsible open={isOpen} onOpenChange={setIsOpen} className="rounded-md border bg-card p-4" asChild>
            <section>
                <div className="flex items-center justify-between gap-3">
                    <Heading as="h3" variant="subsection" className="min-w-0 flex-1">
                        <CollapsibleTrigger
                            ref={triggerRef}
                            className="group flex w-full items-center justify-between gap-3 rounded-sm text-left focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none"
                        >
                            <span className="flex items-center gap-1.5">
                                {title}
                                <FormErrorIcon control={control} paths={paths} onError={() => setIsOpen(true)} />
                            </span>
                            <ChevronDown
                                className="size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180"
                                aria-hidden="true"
                            />
                        </CollapsibleTrigger>
                    </Heading>
                    {isSectionDirty && (
                        <Button
                            type="button"
                            variant="ghost"
                            size="icon-sm"
                            aria-label={`Reset ${title} section`}
                            title={`Reset ${title} section`}
                            onClick={onResetClick}
                        >
                            <RotateCcw aria-hidden="true" />
                        </Button>
                    )}
                </div>
                <CollapsibleContent className="mt-4 grid gap-4">{children}</CollapsibleContent>
            </section>
        </Collapsible>
    );
}
