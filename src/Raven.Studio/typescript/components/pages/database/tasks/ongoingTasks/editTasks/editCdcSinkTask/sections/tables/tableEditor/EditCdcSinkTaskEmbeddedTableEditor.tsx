import { FormGroup, FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { EmbeddedTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext, useWatch } from "react-hook-form";
import EditCdcSinkTaskAdvancedSettings from "./EditCdcSinkTaskAdvancedSettings";
import EditCdcSinkTaskFieldMapping from "./EditCdcSinkTaskFieldMapping";
import EditCdcSinkTaskOnDeleteFields from "./EditCdcSinkTaskOnDeleteFields";
import EditCdcSinkTaskPatchAdvancedField from "./EditCdcSinkTaskPatchAdvancedField";
import FormStringValueList from "components/common/formFields/FormStringValueList";

type CdcSinkRelationType = Raven.Client.Documents.Operations.CdcSink.CdcSinkRelationType;

export default function EditCdcSinkTaskEmbeddedTableEditor({ path }: { path: EmbeddedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const caseSensitiveKeys = useWatch({ control, name: `${path}.caseSensitiveKeys` });
    const patch = useWatch({ control, name: `${path}.patch` });
    const ignoreDeletes = useWatch({ control, name: `${path}.onDelete.ignoreDeletes` });
    const deletePatch = useWatch({ control, name: `${path}.onDelete.patch` });

    const hasAdvancedValues = Boolean(caseSensitiveKeys || patch || ignoreDeletes || deletePatch);

    return (
        <div>
            <div className="grid mb-3">
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Property name</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.propertyName`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Relation type</FormLabel>
                    <FormSelect control={control} name={`${path}.type`} options={relationTypeOptions} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableSchema`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableName`} />
                </FormGroup>
            </div>
            <FormStringValueList
                title="Primary key columns"
                addButtonLabel="Add primary key column"
                control={control}
                name={`${path}.primaryKeyColumns`}
                fieldNameAccessor={(idx) => `${path}.primaryKeyColumns.${idx}.value`}
                defaultValue={{ value: "" }}
                className="mb-2"
            />
            <FormStringValueList
                title="Join columns"
                addButtonLabel="Add join column"
                control={control}
                name={`${path}.joinColumns`}
                fieldNameAccessor={(idx) => `${path}.joinColumns.${idx}.value`}
                defaultValue={{ value: "" }}
                className="mb-2"
            />
            <EditCdcSinkTaskFieldMapping path={path} />
            <EditCdcSinkTaskAdvancedSettings hasAdvancedValues={hasAdvancedValues}>
                <FormGroup>
                    <FormSwitch control={control} name={`${path}.caseSensitiveKeys`}>
                        Case sensitive keys
                    </FormSwitch>
                </FormGroup>
                <EditCdcSinkTaskPatchAdvancedField path={path} />
                <EditCdcSinkTaskOnDeleteFields path={path} />
            </EditCdcSinkTaskAdvancedSettings>
        </div>
    );
}

const relationTypeOptions: SelectOption<CdcSinkRelationType>[] = [
    { value: "Array", label: "Array" },
    { value: "Map", label: "Map" },
    { value: "Value", label: "Value" },
];
