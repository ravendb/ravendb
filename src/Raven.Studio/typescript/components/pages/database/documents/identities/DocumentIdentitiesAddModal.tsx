import { ModalProps } from "reactstrap/types/lib/Modal";
import { Button, InputGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import React, { ComponentPropsWithoutRef, useCallback, useMemo, useState } from "react";
import { Icon } from "components/common/Icon";
import { FormInput } from "components/common/Form";
import { Control, SubmitHandler, useForm, useWatch } from "react-hook-form";
import classNames from "classnames";
import {
    AddIdentitiesFormData,
    addIdentitiesYupResolver,
} from "components/pages/database/documents/identities/DocumentIdentitiesValidation";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

interface DocumentIdentitiesAddModalProps extends ModalProps {
    toggleModal: (value: boolean) => void;
    defaultValues?: AddIdentitiesFormData;
}

export default function DocumentIdentitiesAddModal({
    defaultValues,
    contentClassName,
    wrapClassName,
    ...props
}: DocumentIdentitiesAddModalProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const form = useForm<AddIdentitiesFormData>({
        resolver: addIdentitiesYupResolver,
        defaultValues,
    });
    const [isLoading, setIsLoading] = useState(false);
    const isEditing = !!defaultValues;

    const formValues = useWatch({ control: form.control });
    const onSubmit: SubmitHandler<AddIdentitiesFormData> = useCallback(async ({ prefix, value }) => {
        setIsLoading(true);
        try {
            await databasesService.seedIdentity(databaseName, prefix, value);
            form.reset();
            props.toggleModal(false);
        } catch (e) {
            console.error(e);
        } finally {
            setIsLoading(false);
        }
    }, [databaseName, databasesService, form, props]);

    return (
        <Modal
            contentClassName={classNames("modal-border bulge-primary", contentClassName)}
            wrapClassName={classNames("bs5", wrapClassName)}
            size="lg"
            {...props}
        >
            <form onSubmit={form.handleSubmit(onSubmit)}>
                <ModalBody>
                    <div className="position-absolute m-2 end-0 top-0">
                        <Button close onClick={() => props.toggleModal(false)} />
                    </div>
                    <div className="w-100 d-flex align-items-center justify-content-center flex-column">
                        <Icon size="xl" icon="identities" color="primary" margin="me-0" />
                        <h4>{isEditing ? "Edit Identity" : "Add new identity"}</h4>
                    </div>
                    <div className="w-100 d-flex flex-column gap-4 mb-4">
                        <DocumentIdentitiesAddModalForm isEditing={isEditing} control={form.control} />
                    </div>
                    {formValues?.value && formValues.prefix && <InformationBadge />}
                </ModalBody>
                <ModalFooter>
                    <Button
                        className="border-0 btn-dark text-muted"
                        onClick={() => props.toggleModal(false)}
                        type="button"
                    >
                        Close
                    </Button>
                    <ButtonWithSpinner
                        className="rounded-pill btn-success px-2 py-1"
                        icon="save"
                        isSpinning={isLoading}
                        type="submit"
                    >
                        Save identity
                    </ButtonWithSpinner>
                </ModalFooter>
            </form>
        </Modal>
    );
}

function InformationBadge() {
    return (
        <div
            className={classNames(
                "bg-faded-info rounded-2 mt-3 align-items-center px-2 py-1 d-flex me-2 align-self-start"
            )}
        >
            <Icon icon="info" size="md" color="info" />
            <div className="word-break">
                <p className="mb-0">
                    The effective identity separator in configuration is: <strong>/</strong>
                </p>
                <p className="mb-0">
                    The next document that will be created with prefix &quot;<strong>some_prefix|</strong>&quot; will
                    have ID: &quot;<strong>some_prefix|2</strong>&quot;
                </p>
            </div>
        </div>
    );
}

interface FormFields extends Omit<ComponentPropsWithoutRef<typeof FormInput>, "control"> {
    label: string;
}

interface DocumentIdentitiesAddModalFormProps {
    control: Control<{ prefix?: string; value?: number }>;
    isEditing?: boolean;
}

function DocumentIdentitiesAddModalForm({ control, isEditing }: DocumentIdentitiesAddModalFormProps) {
    const formFields: FormFields[] = useMemo(
        () => [
            {
                type: "text",
                name: "prefix",
                label: "Prefix",
                placeholder: "Enter the document id prefix",
                disabled: isEditing,
            },
            {
                type: "number",
                label: "Value",
                name: "value",
                placeholder: "Enter identity value",
            },
        ],
        [isEditing]
    );

    return (
        <>
            {formFields.map(({ label, ...props }) => (
                <InputGroup className="vstack my-1">
                    <Label>{label}</Label>
                    <FormInput control={control} {...props} />
                </InputGroup>
            ))}
        </>
    );
}
