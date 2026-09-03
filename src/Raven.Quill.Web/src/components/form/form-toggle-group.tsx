import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldContent, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";

export type FormToggleGroupOption<TValue extends string = string> = {
    value: TValue;
    label: ReactNode;
    ariaLabel?: string;
};

type FormToggleGroupProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    /** When false, clicking the selected item keeps it selected instead of clearing to null. */
    canDeselect?: boolean;
    className?: string;
    description?: ReactNode;
    disabled?: boolean;
    label?: ReactNode;
    /** Runs after the form value updates, for view state that follows this field (e.g. a live preview). */
    onValueChange?: (value: string | null) => void;
    options: readonly FormToggleGroupOption[];
    /**
     * "responsive" puts the label and description in a column beside the group, stacking again below
     * the field-group container's `md` breakpoint. Requires a `FieldGroup` ancestor, which owns the
     * container query.
     */
    orientation?: "vertical" | "responsive";
    size?: "sm" | "default" | "lg";
    /** 0 joins the items into one segmented track with shared borders. */
    spacing?: number;
};

/** Single-select toggle group; clicking the selected item again clears the value back to null. */
export function FormToggleGroup<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    canDeselect = true,
    className,
    control,
    defaultValue,
    description,
    disabled,
    label,
    name,
    onValueChange,
    options,
    orientation = "vertical",
    size,
    spacing,
}: FormToggleGroupProps<TFieldValues, TName>) {
    const labelId = useId();
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({
        control,
        defaultValue,
        name,
    });

    const labelNode = label ? <FieldLabel id={labelId}>{label}</FieldLabel> : null;
    const errorNode = error?.message ? (
        <FieldDescription className="text-destructive">{error.message}</FieldDescription>
    ) : null;
    const descriptionNode = description ? <FieldDescription>{description}</FieldDescription> : null;

    const controlNode = (
        // Items never shrink, so a group with enough options to outgrow a narrow column would clip
        // the last ones out of reach. Scrolling keeps them reachable without breaking a joined track
        // into wrapped lines with the wrong corners.
        <div className="no-scrollbar max-w-full min-w-0 overflow-x-auto">
            {/* The group carries its label as an accessible name - without it two groups on one page
                announce identically and there is no way to tell them apart. */}
            <ToggleGroup
                type="single"
                variant="outline"
                size={size}
                spacing={spacing}
                aria-labelledby={label ? labelId : undefined}
                value={typeof value === "string" ? value : ""}
                onValueChange={(next) => {
                    if (next !== "") {
                        onChange(next);
                        onValueChange?.(next);
                    } else if (canDeselect) {
                        onChange(null);
                        onValueChange?.(null);
                    }
                }}
                disabled={disabled || formState.isSubmitting}
                aria-invalid={invalid}
            >
                {options.map((option) => (
                    <ToggleGroupItem key={option.value} value={option.value} aria-label={option.ariaLabel}>
                        {option.label}
                    </ToggleGroupItem>
                ))}
            </ToggleGroup>
        </div>
    );

    if (orientation === "responsive") {
        return (
            <Field orientation="responsive" className={className} data-invalid={invalid}>
                <FieldContent>
                    {labelNode}
                    {errorNode}
                    {descriptionNode}
                </FieldContent>
                {controlNode}
            </Field>
        );
    }

    return (
        <Field className={className} data-invalid={invalid}>
            {labelNode}
            {controlNode}
            {errorNode}
            {descriptionNode}
        </Field>
    );
}
