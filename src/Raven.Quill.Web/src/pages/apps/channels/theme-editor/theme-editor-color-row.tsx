import { Text } from "@/components/typography";
import { useId } from "react";
import { type Control, type FieldPath, useController } from "react-hook-form";
import { ColorPickerPopover } from "@/components/form/color-picker-popover";
import { FieldDescription } from "@/components/shadcn/ui/field";
import { cn } from "@/lib/utils";
import type { WidgetThemeFormData } from "@/pages/apps/channels/web-widget-theme-schema";

type ThemeEditorColorRowProps = {
    control: Control<WidgetThemeFormData>;
    name: FieldPath<WidgetThemeFormData>;
    label: string;
    disabled?: boolean;
    /** One-click anchors for this colour, e.g. the product default and the channel's saved value. */
    presets?: readonly string[];
};

/**
 * One colour as a single filled row that is itself the picker's trigger: its name on the left, its
 * value and swatch on the right. A row rather than a labelled field, because the label, the value and the
 * control are the same three things a field spends three lines saying.
 */
export function ThemeEditorColorRow({ control, name, label, disabled, presets }: ThemeEditorColorRowProps) {
    const errorId = useId();
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
    } = useController({ control, name });

    return (
        <div className="grid gap-1">
            <ColorPickerPopover
                value={typeof value === "string" ? value : ""}
                onChange={onChange}
                disabled={disabled}
                label={label}
                presets={presets}
                showValue
                invalid={invalid}
                describedBy={error?.message ? errorId : undefined}
                // The row itself is the trigger, so anywhere along it opens the picker rather than only
                // the swatch at its end.
                triggerClassName={cn(
                    "h-9 w-full justify-between rounded-lg bg-muted/60 px-3 transition-colors hover:bg-muted active:scale-100",
                    invalid && "ring-1 ring-destructive",
                )}
            >
                <Text as="span" variant="label">
                    {label}
                </Text>
            </ColorPickerPopover>
            {error?.message && (
                <FieldDescription id={errorId} className="text-destructive">
                    {error.message}
                </FieldDescription>
            )}
        </div>
    );
}
