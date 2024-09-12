import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormAceEditor } from "components/common/Form";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import { tryHandleSubmit } from "components/utils/common";
import React, { useState } from "react";
import { SubmitHandler, useForm } from "react-hook-form";
import { Button, CloseButton, Form, Modal, ModalBody, ModalFooter } from "reactstrap";
import endpoints from "endpoints";
import { LazyLoad } from "components/common/LazyLoad";

export default function AdminLogsConfigButton() {
    const { value: isModalOpen, toggle: toggleIsModalOpen } = useBoolean(false);

    return (
        <>
            <Button onClick={toggleIsModalOpen}>
                <Icon icon="config" />
                Configure
            </Button>
            {isModalOpen && <ConfigModal toggle={toggleIsModalOpen} />}
        </>
    );
}

function ConfigModal({ toggle }: { toggle: () => void }) {
    const { control, handleSubmit, formState } = useForm({
        defaultValues: async () => {
            const data = await fetch(endpoints.global.adminLogs.adminLogsConfiguration);
            const jsonData = await data.json();

            return {
                config: JSON.stringify(jsonData, null, 4),
            };
        },
    });

    const handleSave: SubmitHandler<{ config: string }> = ({ config }) => {
        return tryHandleSubmit(async () => {
            await fetch(endpoints.global.adminLogs.adminLogsConfiguration, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: config,
            });

            toggle();
        });
    };

    const [isValid, setIsValid] = useState(false);

    return (
        <Modal isOpen toggle={toggle} wrapClassName="bs5" size="lg" centered>
            <Form onSubmit={handleSubmit(handleSave)}>
                <ModalBody>
                    <div className="d-flex">
                        <h3>Configure admin logs</h3>
                        <CloseButton className="ms-auto" onClick={toggle} />
                    </div>
                    <LazyLoad active={formState.isLoading}>
                        <FormAceEditor
                            control={control}
                            name="config"
                            mode="json"
                            height="500px"
                            setIsValid={setIsValid}
                        />
                    </LazyLoad>
                </ModalBody>
                <ModalFooter className="d-flex justify-content-end gap-1">
                    <Button color="secondary" onClick={toggle}>
                        <Icon icon="close" />
                        Cancel
                    </Button>
                    <ButtonWithSpinner
                        color="primary"
                        type="submit"
                        icon="save"
                        isSpinning={formState.isSubmitting}
                        disabled={formState.isLoading || formState.isSubmitting || !isValid}
                    >
                        Save
                    </ButtonWithSpinner>
                </ModalFooter>
            </Form>
        </Modal>
    );
}
