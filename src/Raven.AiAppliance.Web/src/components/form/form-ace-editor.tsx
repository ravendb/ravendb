import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import AceEditor, { type AceEditorProps } from "@/components/ace-editor/ace-editor";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";

type FormAceEditorProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = Omit<
    AceEditorProps,
    "name" | "onBlur" | "onChange" | "value"
> &
    UseControllerProps<TFieldValues, TName> & {
        description?: ReactNode;
        editorName?: string;
        label?: ReactNode;
    };

export function FormAceEditor<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    editorName,
    label,
    mode,
    name,
    readOnly,
    rules,
    shouldUnregister,
    ...props
}: FormAceEditorProps<TFieldValues, TName>) {
    const generatedId = useId();
    const inputId = editorName ?? generatedId;
    const {
        field: { onBlur, onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({
        control,
        defaultValue,
        disabled,
        name,
        rules,
        shouldUnregister,
    });

    return (
        <Field className={className} data-invalid={invalid}>
            <FieldLabel htmlFor={inputId}>{label}</FieldLabel>
            <AceEditor
                aria-invalid={invalid}
                mode={mode}
                name={inputId}
                onBlur={onBlur}
                onChange={onChange}
                readOnly={readOnly || disabled || formState.isSubmitting}
                validationErrorMessage={error?.message}
                value={value ?? ""}
                {...props}
            />
            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
