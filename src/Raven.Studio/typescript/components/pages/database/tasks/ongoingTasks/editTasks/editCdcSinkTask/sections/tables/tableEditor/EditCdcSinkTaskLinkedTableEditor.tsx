import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import FormStringValueList from "components/common/formFields/FormStringValueList";
import { LinkedTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext } from "react-hook-form";

export default function EditCdcSinkTaskLinkedTableEditor({ path }: { path: LinkedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <div>
            <div className="grid mb-3">
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Property name</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.propertyName`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Linked collection</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.linkedCollectionName`} />
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
                title="Join columns"
                addButtonLabel="Add join column"
                control={control}
                name={`${path}.joinColumns`}
                fieldNameAccessor={(idx) => `${path}.joinColumns.${idx}.value`}
                defaultValue={{ value: "" }}
            />
        </div>
    );
}
