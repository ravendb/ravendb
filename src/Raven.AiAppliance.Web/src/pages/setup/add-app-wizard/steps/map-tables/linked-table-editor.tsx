import { useFormContext } from "react-hook-form";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { LinkedTablePath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { StringListEditor } from "@/pages/setup/add-app-wizard/steps/map-tables/string-list-editor";
import { useSourceTableAutofill } from "@/pages/setup/add-app-wizard/steps/map-tables/use-source-table-autofill";

export function LinkedTableEditor({ path }: { path: LinkedTablePath }) {
    const { control } = useFormContext<AppFormData>();
    const { schemaOptions, tableOptions, handleSourceTableChange } = useSourceTableAutofill(path, "linked");

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
                    description="The document field that holds the reference to the linked document."
                />
                <FormInput
                    control={control}
                    name={`${path}.linkedCollectionName`}
                    label="Linked collection"
                    description="The collection of the related documents. The related document ID is derived from it and the join column values."
                />
            </div>
            <StringListEditor
                name={`${path}.joinColumns`}
                label="Join columns"
                addButtonLabel="Add join column"
                description="Foreign key columns joining this table to the parent. Their values form the related document ID."
            />
        </div>
    );
}
