import type { ReactNode } from "react";
import { useController, useFormContext, type FieldPath } from "react-hook-form";
import { Plus, Trash2 } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { Input } from "@/components/shadcn/ui/input";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

type StringListEditorProps = {
    /** Path to a plain string[] form field. */
    name: FieldPath<AppFormData>;
    label: ReactNode;
    addButtonLabel: string;
    description?: ReactNode;
    placeholder?: string;
};

/** Editor for plain string array fields. FormStringList is not used here because it
 * requires object arrays ({ value }), while the mapping schema stores plain strings. */
export function StringListEditor({ name, label, addButtonLabel, description, placeholder }: StringListEditorProps) {
    const { control } = useFormContext<AppFormData>();
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
    } = useController({ control, name });

    const values = (value ?? []) as string[];
    const errorMessage = error?.message ?? error?.root?.message;

    return (
        <Field>
            <div className="flex items-center justify-between gap-3">
                <FieldLabel>{label}</FieldLabel>
                <Button type="button" variant="ghost" size="sm" onClick={() => onChange([...values, ""])}>
                    <Plus className="size-4" aria-hidden="true" />
                    {addButtonLabel}
                </Button>
            </div>
            {values.length === 0 ? (
                <div className="rounded-md border border-dashed px-3 py-2 text-center text-sm text-muted-foreground">
                    No values.
                </div>
            ) : (
                <div className="grid gap-2">
                    {values.map((item, index) => (
                        <div key={index} className="flex gap-2">
                            <Input
                                value={item}
                                onChange={(e) => onChange(values.map((v, i) => (i === index ? e.target.value : v)))}
                                placeholder={placeholder}
                                aria-invalid={invalid}
                            />
                            <Button
                                type="button"
                                variant="ghost"
                                size="icon"
                                className="text-destructive"
                                onClick={() => onChange(values.filter((_, i) => i !== index))}
                                aria-label="Remove value"
                                title="Remove value"
                            >
                                <Trash2 className="size-4" aria-hidden="true" />
                            </Button>
                        </div>
                    ))}
                </div>
            )}
            {errorMessage && <FieldDescription className="text-destructive">{errorMessage}</FieldDescription>}
            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
