import { yupResolver } from "@hookform/resolvers/yup";
import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import {
    adminLogsActions,
    adminLogsSelectors,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { SubmitHandler, useForm } from "react-hook-form";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import Form from "react-bootstrap/Form";
import * as yup from "yup";

export default function AdminLogsDisplaySettingsModal() {
    const dispatch = useAppDispatch();

    const maxLogsCount = useAppSelector(adminLogsSelectors.maxLogsCount);

    const { control, handleSubmit, reset, formState } = useForm<FormData>({
        defaultValues: {
            maxLogsCount,
        },
        resolver: yupResolver(schema),
    });

    useDirtyFlag(formState.isDirty);

    const handleSave: SubmitHandler<FormData> = (data) => {
        dispatch(adminLogsActions.maxLogsCountSet(data.maxLogsCount));
        reset(data);
        dispatch(adminLogsActions.isDisplaySettingsOpenToggled());
    };

    return (
        <Modal show size="lg" onHide={() => dispatch(adminLogsActions.isDisplaySettingsOpenToggled())}>
            <Form onSubmit={handleSubmit(handleSave)}>
                <Modal.Header onCloseClick={() => dispatch(adminLogsActions.isDisplaySettingsOpenToggled())}>
                    <h3>
                        <Icon icon="client" addon="settings" />
                        Settings - display
                    </h3>
                </Modal.Header>
                <Modal.Body>
                    <FormGroup>
                        <FormLabel>Maximum logs count</FormLabel>
                        <FormInput
                            type="number"
                            control={control}
                            name="maxLogsCount"
                            placeholder="Maximum logs count"
                        />
                    </FormGroup>
                </Modal.Body>
                <Modal.Footer>
                    <Button
                        variant="secondary"
                        type="button"
                        onClick={() => dispatch(adminLogsActions.isDisplaySettingsOpenToggled())}
                    >
                        <Icon icon="cancel" />
                        Close
                    </Button>
                    <Button type="submit" variant="success" disabled={!formState.isDirty}>
                        <Icon icon="save" />
                        Save
                    </Button>
                </Modal.Footer>
            </Form>
        </Modal>
    );
}

const schema = yup.object({
    maxLogsCount: yup.number().min(1).max(200_000).nullable().required(),
});

type FormData = yup.InferType<typeof schema>;
