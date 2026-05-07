import { FormAceEditor, FormGroup, FormLabel } from "components/common/Form";
import {
    EmbeddedTablePath,
    RootTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskFormPaths";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext } from "react-hook-form";

export default function EditCdcSinkTaskPatchAdvancedField({ path }: { path: RootTablePath | EmbeddedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <FormGroup marginClass="mb-0">
            <FormLabel>Patch</FormLabel>
            <FormAceEditor control={control} name={`${path}.patch`} mode="javascript" />
        </FormGroup>
    );
}
