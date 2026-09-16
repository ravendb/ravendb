import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import { RootTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext, useWatch } from "react-hook-form";
import EditCdcSinkTaskAdvancedSettings from "./EditCdcSinkTaskAdvancedSettings";
import EditCdcSinkTaskFieldMapping from "./EditCdcSinkTaskFieldMapping";
import EditCdcSinkTaskOnDeleteFields from "./EditCdcSinkTaskOnDeleteFields";
import EditCdcSinkTaskPatchAdvancedField from "./EditCdcSinkTaskPatchAdvancedField";
import FormStringValueList from "components/common/formFields/FormStringValueList";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";

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
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableSchema`} />
                </FormGroup>
                <FormGroup className="g-col-4">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableName`} />
                </FormGroup>
                <FormGroup className="g-col-4">
                    <FormLabel>
                        Target collection
                        <PopoverWithHoverWrapper message="The RavenDB collection where documents generated from rows in the source table will be stored.">
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput type="text" control={control} name={`${path}.collectionName`} />
                </FormGroup>
            </div>
            <FormStringValueList
                title={
                    <>
                        Primary key columns
                        <PopoverWithHoverWrapper message="Columns that uniquely identify each source row. Their values are used to derive the RavenDB document ID.">
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </>
                }
                addButtonLabel="Add primary key column"
                control={control}
                name={`${path}.primaryKeyColumns`}
                fieldNameAccessor={(idx) => `${path}.primaryKeyColumns.${idx}.value`}
                defaultValue={{ value: "" }}
                className="mb-2"
            />
            <EditCdcSinkTaskFieldMapping path={path} />
            <EditCdcSinkTaskAdvancedSettings hasAdvancedValues={hasAdvancedValues}>
                <EditCdcSinkTaskPatchAdvancedField path={path} />
                <EditCdcSinkTaskOnDeleteFields path={path} />
            </EditCdcSinkTaskAdvancedSettings>
        </div>
    );
}
