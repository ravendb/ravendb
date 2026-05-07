import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import { RootTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskFormPaths";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext, useWatch } from "react-hook-form";
import EditCdcSinkTaskAdvancedSettings from "./EditCdcSinkTaskAdvancedSettings";
import EditCdcSinkTaskFieldMapping from "./EditCdcSinkTaskFieldMapping";
import EditCdcSinkTaskOnDeleteFields from "./EditCdcSinkTaskOnDeleteFields";
import EditCdcSinkTaskPatchAdvancedField from "./EditCdcSinkTaskPatchAdvancedField";
import EditCdcSinkTaskStringValueList from "./EditCdcSinkTaskStringValueList";

export default function EditCdcSinkTaskRootTableEditor({ path }: { path: RootTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const patch = useWatch({ control, name: `${path}.patch` });
    const ignoreDeletes = useWatch({ control, name: `${path}.onDelete.ignoreDeletes` });
    const deletePatch = useWatch({ control, name: `${path}.onDelete.patch` });

    const hasAdvancedValues = Boolean(patch || ignoreDeletes || deletePatch);

    return (
        <div>
            <div className="grid">
                <FormGroup className="g-col-4">
                    <FormLabel>Collection name</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.collectionName`} />
                </FormGroup>
                <FormGroup className="g-col-4">
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableSchema`} />
                </FormGroup>
                <FormGroup className="g-col-4">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableName`} />
                </FormGroup>
            </div>
            <EditCdcSinkTaskStringValueList
                title="Primary key columns"
                addButtonLabel="Add primary key column"
                path={`${path}.primaryKeyColumns`}
            />
            <EditCdcSinkTaskFieldMapping path={path} />
            <EditCdcSinkTaskAdvancedSettings hasAdvancedValues={hasAdvancedValues}>
                <EditCdcSinkTaskPatchAdvancedField path={path} />
                <EditCdcSinkTaskOnDeleteFields path={path} />
            </EditCdcSinkTaskAdvancedSettings>
        </div>
    );
}
