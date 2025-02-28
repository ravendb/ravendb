import Button from "react-bootstrap/Button";
import { CloseButton, FormGroup, InputGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import React from "react";
import { Icon } from "components/common/Icon";
import { FormInput } from "components/common/Form";
import { Control, SubmitHandler, useForm, useWatch } from "react-hook-form";
import {
    AddIdentitiesFormData,
    DocumentIdentitiesPrefixTestContext,
    documentIdentitiesSchema,
} from "components/pages/database/documents/identities/DocumentIdentitiesValidation";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { tryHandleSubmit } from "components/utils/common";
import RichAlert from "components/common/RichAlert";
import { useAsync } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { yupResolver } from "@hookform/resolvers/yup";
import { useEventsCollector } from "hooks/useEventsCollector";

interface DocumentIdentitiesModalProps {
    toggleModal: () => void;
    defaultValues?: AddIdentitiesFormData;
    identities?: AddIdentitiesFormData[];
    refetch: () => void;
    isOpen: boolean;
}

export default function DocumentIdentitiesModal({
    defaultValues,
    refetch,
    identities,
    toggleModal,
    ...props
}: DocumentIdentitiesModalProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const isEditing = !!defaultValues;
    const eventsCollector = useEventsCollector();
    const {
        reset,
        handleSubmit,
        control,
        formState: { isSubmitting },
    } = useForm<AddIdentitiesFormData, DocumentIdentitiesPrefixTestContext>({
        context: {
            identities,
            isEditing,
        },
        resolver: yupResolver(documentIdentitiesSchema),
        defaultValues,
    });

    const formValues = useWatch({ control });

    const onSubmit: SubmitHandler<AddIdentitiesFormData> = ({ prefix, value }) => {
        return tryHandleSubmit(async () => {
            await databasesService.seedIdentity(databaseName, prefix, value);
            if (!isEditing) {
                eventsCollector.reportEvent("identity", "new");
            }
            refetch();
            toggleModal();
            reset();
        });
    };

    return (
        <Modal centered contentClassName="modal-border bulge-primary" wrapClassName="bs5" size="lg" {...props}>
            <form onSubmit={handleSubmit(onSubmit)}>
                <ModalBody className="pb-0 vstack gap-3">
                    <div className="position-absolute m-2 end-0 top-0">
                        <CloseButton onClick={toggleModal} />
                    </div>
                    <div className="text-center">
                        <Icon icon="identities" color="primary" margin="me-0" className="fs-1" />
                    </div>
                    <div className="text-center lead">{isEditing ? "Edit identity" : "Add new identity"}</div>
                    <DocumentIdentitiesModalForm isEditing={isEditing} control={control} />
                    <InformationBadge isEditing={isEditing} {...formValues} />
                </ModalBody>
                <ModalFooter className="mt-4">
                    <Button className="link-muted" variant="link" onClick={toggleModal} type="button">
                        Close
                    </Button>
                    <ButtonWithSpinner
                        className="rounded-pill"
                        variant="success"
                        icon="save"
                        isSpinning={isSubmitting}
                        type="submit"
                        title="Save identity"
                    >
                        Save identity
                    </ButtonWithSpinner>
                </ModalFooter>
            </form>
        </Modal>
    );
}

interface InformationBadgeProps extends AddIdentitiesFormData {
    isEditing: boolean;
}

function InformationBadge({ prefix = "<Prefix>", value, isEditing }: InformationBadgeProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { manageServerService } = useServices();
    const { result: identityPartsSeparator, loading } = useAsync(async () => {
        try {
            const clientConfiguration = manageServerService.getClientConfiguration(databaseName);
            const globalClientConfiguration = manageServerService.getGlobalClientConfiguration();

            const [clientConfig, globalConfig] = await Promise.all([clientConfiguration, globalClientConfiguration]);

            if (!clientConfig?.Disabled && clientConfig?.IdentityPartsSeparator != null) {
                return clientConfig.IdentityPartsSeparator;
            }

            if (!globalConfig?.Disabled && globalConfig?.IdentityPartsSeparator != null) {
                return globalConfig.IdentityPartsSeparator;
            }

            return "/";
        } catch (error) {
            console.error("Error fetching configurations:", error);
            return "/";
        }
    }, []);

    const formattedPrefix = isEditing ? prefix.slice(0, -1) : prefix;

    return (
        <RichAlert icon="info" variant="info">
            <div className="word-break">
                <p className="mb-0">
                    The effective identity separator in configuration is:{" "}
                    <code>
                        <LazyLoad active={loading}>{identityPartsSeparator}</LazyLoad>
                    </code>
                </p>
                <p className="mb-0">
                    The next document that will be created with prefix &quot;
                    <strong>{formattedPrefix}|</strong>
                    &quot; will have ID: &quot;
                    <code>
                        {formattedPrefix}
                        {identityPartsSeparator}
                        {value ? value + 1 : `<Value + 1>`}
                    </code>
                    &quot;
                </p>
            </div>
        </RichAlert>
    );
}

interface DocumentIdentitiesModalFormProps {
    control: Control<AddIdentitiesFormData>;
    isEditing?: boolean;
}

function DocumentIdentitiesModalForm({ control, isEditing }: DocumentIdentitiesModalFormProps) {
    return (
        <FormGroup className="vstack gap-3">
            <InputGroup className="vstack">
                <Label>Prefix</Label>
                <FormInput
                    name="prefix"
                    type="text"
                    control={control}
                    placeholder="Enter the document id prefix"
                    disabled={isEditing}
                />
            </InputGroup>
            <InputGroup className="vstack">
                <Label>Value</Label>
                <FormInput name="value" type="number" control={control} placeholder="Enter identity value" />
            </InputGroup>
        </FormGroup>
    );
}
