import { FormAceEditor, FormGroup, FormLabel, FormSwitch } from "components/common/Form";
import {
    EmbeddedTablePath,
    RootTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext, useWatch } from "react-hook-form";

export default function EditCdcSinkTaskOnDeleteFields({ path }: { path: RootTablePath | EmbeddedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const isIgnoreDeletes = useWatch({ control, name: `${path}.onDelete.ignoreDeletes` });

    return (
        <div>
            <FormGroup>
                <FormSwitch control={control} name={`${path}.onDelete.ignoreDeletes`}>
                    Ignore deletes
                </FormSwitch>
            </FormGroup>
            <FormGroup marginClass="mb-0">
                <FormLabel>Delete patch</FormLabel>
                <FormAceEditor
                    control={control}
                    name={`${path}.onDelete.patch`}
                    mode="javascript"
                    disabled={Boolean(isIgnoreDeletes)}
                />
            </FormGroup>
        </div>
    );
}
