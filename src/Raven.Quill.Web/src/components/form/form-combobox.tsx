import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import {
    Combobox,
    ComboboxContent,
    ComboboxEmpty,
    ComboboxInput,
    ComboboxItem,
    ComboboxList,
} from "@/components/shadcn/ui/combobox";
import { cn } from "@/lib/utils";

export type FormComboboxOption<T extends string | number | boolean> = {
    value: T;
    label: string;
    disabled?: boolean;
};

type FormComboboxProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    className?: string;
    description?: ReactNode;
    disabled?: boolean;
    emptyMessage?: ReactNode;
    inputClassName?: string;
    label?: ReactNode;
    options: readonly FormComboboxOption<TFieldValues[TName]>[];
    placeholder?: string;
    addons?: ReactNode;
};

export function FormCombobox<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    emptyMessage = "No results found.",
    inputClassName,
    label,
    name,
    options,
    placeholder,
    addons,
}: FormComboboxProps<TFieldValues, TName>) {
    const generatedId = useId();
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({
        control,
        defaultValue,
        name,
    });

    const selectedOption = options.find((option) => option.value === value) ?? null;

    return (
        <Field className={className} data-invalid={invalid}>
            <FieldLabel htmlFor={generatedId}>{label}</FieldLabel>
            <div className="flex items-center gap-2">
                <Combobox
                    items={options}
                    itemToStringValue={(option) => option.label}
                    value={selectedOption}
                    onValueChange={(option) => onChange(option ? option.value : "")}
                    disabled={disabled || formState.isSubmitting}
                >
                    <ComboboxInput
                        id={generatedId}
                        placeholder={placeholder}
                        aria-invalid={invalid}
                        showClear
                        className={cn("w-full", inputClassName)}
                    />
                    <ComboboxContent>
                        <ComboboxEmpty>{emptyMessage}</ComboboxEmpty>
                        <ComboboxList>
                            <ComboboxList>
                                {(item) => (
                                    <ComboboxItem key={item.value} value={item}>
                                        {item.label}
                                    </ComboboxItem>
                                )}
                            </ComboboxList>
                        </ComboboxList>
                    </ComboboxContent>
                </Combobox>
                {addons && addons}
            </div>
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
