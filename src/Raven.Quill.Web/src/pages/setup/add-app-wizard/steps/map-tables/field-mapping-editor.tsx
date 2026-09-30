import { useState } from "react";
import { ArrowRight, Plus, Trash2 } from "lucide-react";
import { useFieldArray, useFormContext, type FieldArrayPath, type FieldPath } from "react-hook-form";
import type { CdcColumnType } from "@/api/generated/server-api";
import { ExpandableList } from "@/components/data/expandable-list";
import { FormErrorIcon } from "@/components/form/form-error-icon";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { Text } from "@/components/typography";
import { Button } from "@/components/shadcn/ui/button";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { EmbeddedTablePath, RootTablePath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";

const COLUMN_TYPE_OPTIONS: FormSelectOption<CdcColumnType>[] = [
    { value: "Default", label: "Default" },
    { value: "Json", label: "JSON" },
    { value: "Attachment", label: "Attachment" },
];

/** Tables can map hundreds of columns; rendering an input row per mapping makes the editor
 * sluggish, so the list starts collapsed to this many rows. */
const COLLAPSED_MAPPINGS_COUNT = 6;

const ROW_GRID_CLASS = "grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)_7rem_2rem] items-start gap-2";

type FieldMappingEditorProps = {
    path: RootTablePath | EmbeddedTablePath;
};

export function FieldMappingEditor({ path }: FieldMappingEditorProps) {
    const { control } = useFormContext<AppFormData>();
    const [isExpanded, setIsExpanded] = useState(false);
    const columnsPath = `${path}.columns`;

    const columnsFieldArray = useFieldArray({
        control,
        name: columnsPath as FieldArrayPath<AppFormData>,
    });

    const addFieldMapping = () => {
        columnsFieldArray.append({ column: "", name: "", type: "Default" });
        setIsExpanded(true);
    };

    return (
        <div className="grid gap-2">
            <div className="flex items-center justify-between gap-3">
                <Text variant="label" as="div" className="flex items-center gap-1.5">
                    Field mapping
                    <FormErrorIcon control={control} paths={[columnsPath as FieldPath<AppFormData>]} />
                </Text>
                <Button type="button" variant="ghost" size="sm" onClick={addFieldMapping}>
                    <Plus className="size-4" aria-hidden="true" />
                    Add field mapping
                </Button>
            </div>
            {columnsFieldArray.fields.length === 0 ? (
                <Text variant="muted" as="div" className="rounded-md border border-dashed px-3 py-2 text-center">
                    No field mappings defined.
                </Text>
            ) : (
                <div className="grid gap-2">
                    <div className={`${ROW_GRID_CLASS} text-xs text-muted-foreground`}>
                        <div>Source column</div>
                        <div className="w-4" />
                        <div>Target property</div>
                        <div>Type</div>
                        <div />
                    </div>
                    <ExpandableList
                        className="grid gap-2"
                        itemsCount={columnsFieldArray.fields.length}
                        collapsedItemsCount={COLLAPSED_MAPPINGS_COUNT}
                        isExpanded={isExpanded}
                        setIsExpanded={setIsExpanded}
                    >
                        {({ visibleCount }) =>
                            columnsFieldArray.fields.slice(0, visibleCount).map((field, index) => (
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
                            ))
                        }
                    </ExpandableList>
                </div>
            )}
        </div>
    );
}
