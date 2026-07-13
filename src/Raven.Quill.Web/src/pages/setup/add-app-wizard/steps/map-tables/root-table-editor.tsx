import { useFormContext } from "react-hook-form";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { AdvancedSettings } from "@/pages/setup/add-app-wizard/steps/map-tables/advanced-settings";
import { FieldMappingEditor } from "@/pages/setup/add-app-wizard/steps/map-tables/field-mapping-editor";
import type { RootTablePath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { FormStringList } from "@/components/form/form-string-list";
import { useSourceTableAutofill } from "@/pages/setup/add-app-wizard/steps/map-tables/use-source-table-autofill";

export function RootTableEditor({ path }: { path: RootTablePath }) {
    const { control } = useFormContext<AppFormData>();
    const { schemaOptions, tableOptions, handleSourceTableChange } = useSourceTableAutofill(path, "root");

    return (
        <div className="grid gap-5">
            <div className="grid gap-4 lg:grid-cols-3">
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
                    name={`${path}.collectionName`}
                    label="Target collection"
                    description="The RavenDB collection where documents generated from this table are stored."
                />
            </div>
            <FormStringList
                control={control}
                name={`${path}.primaryKeyColumns`}
                fieldName={(index) => `${path}.primaryKeyColumns.${index}.value`}
                defaultValue={{ value: "" }}
                label="Primary key columns"
                addButtonLabel="Add primary key column"
                description="Columns that uniquely identify each source row. Their values derive the document ID."
            />
            <FieldMappingEditor path={path} />
            <AdvancedSettings path={path} />
        </div>
    );
}
