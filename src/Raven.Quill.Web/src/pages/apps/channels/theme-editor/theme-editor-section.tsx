import { ChevronDown, RotateCcw } from "lucide-react";
import { useRef, useState, type ReactNode } from "react";
import { useFormState, useWatch, type Control } from "react-hook-form";
import { FormErrorIcon } from "@/components/form/form-error-icon";
import { Button } from "@/components/shadcn/ui/button";
import { Heading } from "@/components/typography";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { cn } from "@/lib/utils";
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

/** Values are hex strings, booleans, numbers, or the suggested prompts' array of objects. */
function isSameValue(current: unknown, saved: unknown): boolean {
    return current === saved || JSON.stringify(current ?? null) === JSON.stringify(saved ?? null);
}

export function ThemeEditorSection({
    title,
    control,
    paths,
    onReset,
    defaultOpen = false,
    children,
}: ThemeEditorSectionProps) {
    const [isOpen, setIsOpen] = useState(defaultOpen);
    // The height animation needs the content clipped, but leaving it clipped once the section sits
    // open cuts the focus rings of the fields at its edges, so only clip while the animation runs.
    const [isAnimating, setIsAnimating] = useState(false);

    const toggleSection = (next: boolean) => {
        setIsAnimating(true);
        setIsOpen(next);
    };
    // Only offer the undo once there is something to undo, so an untouched section stays quiet. This
    // compares the section's values against the form's defaults rather than reading `dirtyFields`:
    // resetField leaves this form's entries in `dirtyFields` behind (the form is seeded through
    // `values`, and the re-seed keeps them), which left every section's undo button on screen after
    // the undo had already happened.
    const { defaultValues } = useFormState({ control });
    const values = useWatch({ control, name: [...paths] });
    const isSectionDirty = paths.some((path, index) => !isSameValue(values[index], defaultValues?.[path]));
    const triggerRef = useRef<HTMLButtonElement>(null);

    const onResetClick = () => {
        onReset(paths);
        // The reset button only renders while the section is dirty, so this click unmounts it -
        // without moving focus deliberately, the browser drops it to <body>, which for a keyboard or
        // screen-reader user reads as being bounced to the top of the document with no announcement.
        triggerRef.current?.focus();
    };

    return (
        <Collapsible open={isOpen} onOpenChange={toggleSection} className="p-4" asChild>
            <section>
                {/* The row keeps the undo button's height even while the button is hidden, so a section
                    turning dirty does not grow the header and nudge the title. */}
                <div className="flex min-h-7 items-center justify-between gap-3">
                    <Heading as="h3" variant="subsection" className="min-w-0 flex-1">
                        <CollapsibleTrigger
                            ref={triggerRef}
                            className="group flex w-full items-center justify-between gap-3 rounded-sm text-left focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none"
                        >
                            <span className="flex items-center gap-1.5">
                                {title}
                                <FormErrorIcon control={control} paths={paths} onError={() => toggleSection(true)} />
                            </span>
                            <ChevronDown
                                className="size-4 shrink-0 text-muted-foreground transition-transform duration-200 group-data-[state=open]:rotate-180"
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
                <CollapsibleContent
                    onAnimationEnd={(event) => {
                        if (event.target === event.currentTarget) {
                            setIsAnimating(false);
                        }
                    }}
                    className={cn(
                        isAnimating && "overflow-hidden",
                        "data-[state=closed]:animate-collapsible-up data-[state=open]:animate-collapsible-down",
                    )}
                >
                    <div className="mt-4 grid gap-4">{children}</div>
                </CollapsibleContent>
            </section>
        </Collapsible>
    );
}
