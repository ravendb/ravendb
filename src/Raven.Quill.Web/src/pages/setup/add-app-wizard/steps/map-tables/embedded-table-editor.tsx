import { useFormContext } from "react-hook-form";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormSwitch } from "@/components/form/form-switch";
import type { CdcSinkRelationType } from "@/api/generated/server-api";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { AddNestedTableButtons } from "@/pages/setup/add-app-wizard/steps/map-tables/add-nested-table-buttons";
import { AdvancedSettings } from "@/pages/setup/add-app-wizard/steps/map-tables/advanced-settings";
import { FieldMappingEditor } from "@/pages/setup/add-app-wizard/steps/map-tables/field-mapping-editor";
import type { EmbeddedTablePath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { FormStringList } from "@/components/form/form-string-list";
import { useSourceTableAutofill } from "@/pages/setup/add-app-wizard/steps/map-tables/use-source-table-autofill";

const RELATION_TYPE_OPTIONS: FormSelectOption<CdcSinkRelationType>[] = [
    { value: "Array", label: "Array - multiple rows stored as an array of objects" },
    { value: "Map", label: "Map - multiple rows stored as a keyed object" },
    { value: "Value", label: "Value - a single row stored directly as an object" },
];

export function EmbeddedTableEditor({ path }: { path: EmbeddedTablePath }) {
    const { control } = useFormContext<AppFormData>();
    const { schemaOptions, tableOptions, handleSourceTableChange } = useSourceTableAutofill(path, "embedded");

    return (
        <div className="grid gap-5">
            <div className="grid gap-4 lg:grid-cols-2">
                <FormAutocomplete
                    control={control}
                    name={`${path}.sourceTableSchema`}
                    label="Source schema"
                    options={schemaOptions}
                    placeholder="Select or enter source schema"
                />
                <FormAutocomplete
                    control={control}
                    name={`${path}.sourceTableName`}
                    label="Source table"
                    options={tableOptions}
                    placeholder="Select or enter source table"
                    afterChange={handleSourceTableChange}
                />
                <FormInput
                    control={control}
                    name={`${path}.propertyName`}
                    label="Target property"
                    description="The parent document field where the embedded data is stored."
                />
                <FormSelect
                    control={control}
                    name={`${path}.type`}
                    label="Relation type"
                    options={RELATION_TYPE_OPTIONS}
                />
            </div>
            <FormStringList
                control={control}
                name={`${path}.primaryKeyColumns`}
                fieldName={(index) => `${path}.primaryKeyColumns.${index}.value`}
                defaultValue={{ value: "" }}
                label="Primary key columns"
                addButtonLabel="Add primary key column"
                description="Columns that uniquely identify rows in this related table."
            />
            <FormStringList
                control={control}
                name={`${path}.joinColumns`}
                fieldName={(index) => `${path}.joinColumns.${index}.value`}
                defaultValue={{ value: "" }}
                label="Join columns"
                addButtonLabel="Add join column"
                description="Columns used to match rows in this related table with rows from the parent table."
            />
            <AddNestedTableButtons path={path} />
            <FieldMappingEditor path={path} />
            <AdvancedSettings path={path}>
                <FormSwitch control={control} name={`${path}.caseSensitiveKeys`} label="Case sensitive keys" />
            </AdvancedSettings>
        </div>
    );
}
