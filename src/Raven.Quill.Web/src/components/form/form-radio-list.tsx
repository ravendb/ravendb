import { type ReactNode } from "react";
import { type Control, type FieldPath, type FieldValues, useController } from "react-hook-form";
import { SELECTED_CARD_CLASSES } from "@/components/form/form-radio-cards";
import { FieldDescription } from "@/components/shadcn/ui/field";
import { Input } from "@/components/shadcn/ui/input";
import { RadioGroup, RadioGroupItem } from "@/components/shadcn/ui/radio-group";
import { Text } from "@/components/typography";
import { cn } from "@/lib/utils";

const ROW_CLASSES =
    "grid cursor-pointer grid-cols-[1rem_minmax(0,1fr)_auto] items-center gap-3 rounded-md border px-3 py-2.25";

export type RadioListOption = {
    value: string;
    label: ReactNode;
    /** Shown at the row's end, e.g. the key that picks it. */
    hint?: ReactNode;
};

/** The last option, answered in the person's own words. Typing in it picks it; landing on it does not. */
export type RadioListOther<TFieldValues extends FieldValues> = {
    name: FieldPath<TFieldValues>;
    value: string;
    label: string;
    placeholder: string;
    hint?: ReactNode;
};

type FormRadioListProps<TFieldValues extends FieldValues> = {
    control: Control<TFieldValues>;
    name: FieldPath<TFieldValues>;
    options: RadioListOption[];
    other?: RadioListOther<TFieldValues>;
    className?: string;
    "aria-labelledby"?: string;
};

export function FormRadioList<TFieldValues extends FieldValues>({
    control,
    name,
    options,
    other,
    className,
    "aria-labelledby": labelledBy,
}: FormRadioListProps<TFieldValues>) {
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
    } = useController({ control, name });

    return (
        <RadioGroup
            aria-labelledby={labelledBy}
            aria-invalid={invalid || undefined}
            value={value}
            className={cn("flex flex-col gap-1", className)}
            onValueChange={onChange}
        >
            {options.map((option) => (
                <label key={option.value} className={cn(ROW_CLASSES, value === option.value && SELECTED_CARD_CLASSES)}>
                    <RadioGroupItem value={option.value} />
                    <Text as="span">{option.label}</Text>
                    {option.hint}
                </label>
            ))}
            {other && (
                <OtherOption
                    control={control}
                    other={other}
                    isSelected={value === other.value}
                    onSelect={() => onChange(other.value)}
                />
            )}
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
        </RadioGroup>
    );
}

function OtherOption<TFieldValues extends FieldValues>({
    control,
    other,
    isSelected,
    onSelect,
}: {
    control: Control<TFieldValues>;
    other: RadioListOther<TFieldValues>;
    isSelected: boolean;
    onSelect: () => void;
}) {
    const {
        field: { onBlur, onChange, ref, value },
        fieldState: { error },
    } = useController({ control, name: other.name });
    // An error can be left over from an earlier attempt to move on; it only counts while the write-in is the pick.
    const pickedError = isSelected ? error : undefined;

    return (
        <>
            <div className={cn(ROW_CLASSES, "cursor-text", isSelected && SELECTED_CARD_CLASSES)}>
                <RadioGroupItem value={other.value} aria-label={other.label} />
                <Input
                    ref={ref}
                    value={value}
                    aria-label={other.label}
                    aria-invalid={pickedError !== undefined}
                    placeholder={other.placeholder}
                    className="h-auto rounded-none border-0 bg-transparent p-0 focus-visible:ring-0 aria-invalid:ring-0 dark:bg-transparent"
                    onBlur={onBlur}
                    onChange={(event) => {
                        onChange(event);
                        onSelect();
                    }}
                />
                {other.hint}
            </div>
            {pickedError?.message && (
                <FieldDescription className="text-destructive">{pickedError.message}</FieldDescription>
            )}
        </>
    );
}
