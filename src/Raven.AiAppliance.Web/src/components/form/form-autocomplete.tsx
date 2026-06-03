import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import {
    Autocomplete,
    AutocompleteContent,
    AutocompleteEmpty,
    AutocompleteInput,
    AutocompleteItem,
    AutocompleteList,
} from "@/components/shadcn/ui/autocomplete";
import { cn } from "@/lib/utils";

type FormAutocompleteProps<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
> = UseControllerProps<TFieldValues, TName> & {
    className?: string;
    description?: ReactNode;
    disabled?: boolean;
    emptyMessage?: ReactNode;
    inputClassName?: string;
    label?: ReactNode;
    /** Suggested values. The field stays free-text, so any typed value is kept. */
    options: readonly string[];
    placeholder?: string;
};

/**
 * A text field that suggests options as the user types while keeping whatever they type.
 * Use it when the full set of valid values is unknown (e.g. an AI model name) but a few
 * common choices are worth surfacing.
 */
export function FormAutocomplete<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    emptyMessage = "No suggestions.",
    inputClassName,
    label,
    name,
    options,
    placeholder,
}: FormAutocompleteProps<TFieldValues, TName>) {
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

    const isDisabled = disabled || formState.isSubmitting;

    // TODO fix selecting autocomplete item

    return (
        <Field className={className} data-invalid={invalid}>
            <FieldLabel htmlFor={generatedId}>{label}</FieldLabel>
            <Autocomplete
                items={options}
                value={value ?? ""}
                onValueChange={onChange}
                disabled={isDisabled}
                openOnInputClick
            >
                <AutocompleteInput
                    id={generatedId}
                    placeholder={placeholder}
                    aria-invalid={invalid}
                    disabled={isDisabled}
                    showClear={!!value}
                    className={cn("w-full", inputClassName)}
                />
                <AutocompleteContent>
                    <AutocompleteEmpty>{emptyMessage}</AutocompleteEmpty>
                    <AutocompleteList>
                        {(item: string) => (
                            <AutocompleteItem key={item} value={item}>
                                {item}
                            </AutocompleteItem>
                        )}
                    </AutocompleteList>
                </AutocompleteContent>
            </Autocomplete>
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
