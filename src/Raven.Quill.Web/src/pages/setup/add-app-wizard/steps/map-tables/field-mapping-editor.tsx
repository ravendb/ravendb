// React Compiler memoization is disabled here: the array-level error message is derived
// from react-hook-form's mutable errors object, which keeps a stable identity across updates.
"use no memo";

import { ArrowRight, Plus, Trash2 } from "lucide-react";
import { useFieldArray, useFormContext, useFormState, type FieldArrayPath, type FieldPath } from "react-hook-form";
import type { CdcColumnType } from "@/api/generated/server-api";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { Button } from "@/components/shadcn/ui/button";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { EmbeddedTablePath, RootTablePath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { getErrorAtPath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";

const COLUMN_TYPE_OPTIONS: FormSelectOption<CdcColumnType>[] = [
    { value: "Default", label: "Default" },
    { value: "Json", label: "JSON" },
    { value: "Attachment", label: "Attachment" },
];

const ROW_GRID_CLASS = "grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)_7rem_2rem] items-start gap-2";

type FieldMappingEditorProps = {
    path: RootTablePath | EmbeddedTablePath;
};

export function FieldMappingEditor({ path }: FieldMappingEditorProps) {
    const { control } = useFormContext<AppFormData>();
    const columnsPath = `${path}.columns`;

    const columnsFieldArray = useFieldArray({
        control,
        name: columnsPath as FieldArrayPath<AppFormData>,
    });

    const { errors } = useFormState({ control, name: columnsPath as FieldPath<AppFormData> });
    const columnsError = getErrorAtPath(errors, columnsPath) as { message?: string; root?: { message?: string } };
    const errorMessage = columnsError?.message ?? columnsError?.root?.message;

    return (
        <div className="grid gap-2">
            <div className="flex items-center justify-between gap-3">
                <div className="text-sm font-medium">Field mapping</div>
                <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => columnsFieldArray.append({ column: "", name: "", type: "Default" })}
                >
                    <Plus className="size-4" aria-hidden="true" />
                    Add field mapping
                </Button>
            </div>
            {columnsFieldArray.fields.length === 0 ? (
                <div className="rounded-md border border-dashed px-3 py-2 text-center text-sm text-muted-foreground">
                    No field mappings defined.
                </div>
            ) : (
                <div className="grid gap-2">
                    <div className={`${ROW_GRID_CLASS} text-xs text-muted-foreground`}>
                        <div>Source column</div>
                        <div className="w-4" />
                        <div>Target property</div>
                        <div>Type</div>
                        <div />
                    </div>
                    {columnsFieldArray.fields.map((field, index) => (
                        <div key={field.id} className={ROW_GRID_CLASS}>
                            <FormInput
                                control={control}
                                name={`${columnsPath}.${index}.column` as FieldPath<AppFormData>}
                            />
                            <ArrowRight className="mt-2.5 size-4 text-muted-foreground" aria-hidden="true" />
                            <FormInput
                                control={control}
                                name={`${columnsPath}.${index}.name` as FieldPath<AppFormData>}
                            />
                            <FormSelect
                                control={control}
                                name={`${columnsPath}.${index}.type` as FieldPath<AppFormData>}
                                options={COLUMN_TYPE_OPTIONS}
                            />
                            <Button
                                type="button"
                                variant="ghost"
                                size="icon"
                                className="text-destructive"
                                onClick={() => columnsFieldArray.remove(index)}
                                aria-label="Remove field mapping"
                                title="Remove field mapping"
                            >
                                <Trash2 className="size-4" aria-hidden="true" />
                            </Button>
                        </div>
                    ))}
                </div>
            )}
            {errorMessage && <p className="text-sm text-destructive">{errorMessage}</p>}
        </div>
    );
}
