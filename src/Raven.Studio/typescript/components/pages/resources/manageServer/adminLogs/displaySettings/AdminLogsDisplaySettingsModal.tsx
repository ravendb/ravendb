import { yupResolver } from "@hookform/resolvers/yup";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import {
    adminLogsSelectors,
    adminLogsActions,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppSelector, useAppDispatch } from "components/store";
import { useForm, SubmitHandler } from "react-hook-form";
import Button from "react-bootstrap/Button";
import { Modal, ModalBody, CloseButton, FormGroup, Label, Form } from "reactstrap";
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
        <Modal isOpen wrapClassName="bs5" centered size="lg">
            <ModalBody>
                <div className="d-flex">
                    <h3>
                        <Icon icon="client" addon="settings" />
                        Settings - display
                    </h3>
                    <CloseButton
                        className="ms-auto"
                        onClick={() => dispatch(adminLogsActions.isDisplaySettingsOpenToggled())}
                    />
                </div>

                <Form onSubmit={handleSubmit(handleSave)}>
                    <FormGroup>
                        <Label>Maximum logs count</Label>
                        <FormInput
                            type="number"
                            control={control}
                            name="maxLogsCount"
                            placeholder="Maximum logs count"
                        />
                    </FormGroup>

                    <div className="d-flex justify-content-end gap-2">
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
                    </div>
                </Form>
            </ModalBody>
        </Modal>
    );
}

const schema = yup.object({
    maxLogsCount: yup.number().min(1).max(200_000).nullable().required(),
});

type FormData = yup.InferType<typeof schema>;
