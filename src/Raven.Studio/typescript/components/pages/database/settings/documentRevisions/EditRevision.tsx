import { Icon } from "components/common/Icon";
import React from "react";
import { Alert, Button, Form, InputGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import { FormInput, FormSelect, FormSwitch } from "components/common/Form";
import { SubmitHandler, useForm } from "react-hook-form";
import {
    EditDocumentRevisionsCollectionConfig,
    documentRevisionsCollectionConfigYupResolver,
    documentRevisionsConfigYupResolver,
} from "components/pages/database/settings/documentRevisions/DocumentRevisionsValidation";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import assertUnreachable from "components/utils/assertUnreachable";
import { DocumentRevisionsConfig, DocumentRevisionsConfigName } from "./store/documentRevisionsSlice";
import useEditRevisionFormController from "./useEditRevisionFormController";

export type EditRevisionConfigType = "defaultDocument" | "defaultConflicts" | "collectionSpecific";
export type EditRevisionTaskType = "edit" | "new";

interface EditRevisionProps {
    toggle: () => void;
    onConfirm: (config: DocumentRevisionsConfig) => void;
    configType: EditRevisionConfigType;
    taskType: EditRevisionTaskType;
    config?: DocumentRevisionsConfig;
}

export default function EditRevision(props: EditRevisionProps) {
    const { toggle, configType, taskType, config, onConfirm } = props;

    const { control, formState, setValue, handleSubmit } = useForm<EditDocumentRevisionsCollectionConfig>({
        resolver:
            configType === "collectionSpecific" && taskType === "new"
                ? documentRevisionsCollectionConfigYupResolver
                : documentRevisionsConfigYupResolver,
        mode: "all",
        defaultValues: getInitialValues(config),
    });

    const formValues = useEditRevisionFormController(control, setValue);
    useDirtyFlag(formState.isDirty);

    const onSubmit: SubmitHandler<EditDocumentRevisionsCollectionConfig> = (formData) => {
        onConfirm(mapToDocumentRevisionsConfig(formData, configType));
        toggle();
    };

    // TODO kalczur - get collections in parent
    const collectionOptions = [
        { value: "collection1", label: "Collection 1" },
        { value: "collection2", label: "Collection 2" },
        { value: "collection3", label: "Collection 3" },
    ];

    return (
        <Modal isOpen toggle={toggle} wrapClassName="bs5" contentClassName="modal-border bulge-info">
            <Form onSubmit={handleSubmit(onSubmit)} autoComplete="off">
                <ModalBody className="vstack gap-2">
                    <h4>{getTitle(taskType, configType)}</h4>
                    {configType === "collectionSpecific" && (
                        <InputGroup className="gap-1 flex-wrap flex-column">
                            <Label className="mb-0 md-label">Collection</Label>
                            <FormSelect control={control} name="CollectionName" options={collectionOptions} />
                        </InputGroup>
                    )}
                    <FormSwitch control={control} name="IsPurgeOnDeleteEnabled">
                        Purge revisions on document delete
                    </FormSwitch>
                    <FormSwitch control={control} name="IsMinimumRevisionsToKeepEnabled">
                        Limit # of revisions to keep
                    </FormSwitch>
                    {formValues.IsMinimumRevisionsToKeepEnabled && (
                        <InputGroup className="mb-2">
                            <FormInput
                                type="number"
                                control={control}
                                name="MinimumRevisionsToKeep"
                                placeholder="Enter number of revisions to keep"
                            />
                        </InputGroup>
                    )}
                    <FormSwitch control={control} name="IsMinimumRevisionAgeToKeepEnabled">
                        Limit # of revisions to keep by age
                    </FormSwitch>
                    {formValues.IsMinimumRevisionAgeToKeepEnabled && (
                        <InputGroup className="mb-2 d-flex">
                            <FormInput
                                type="text"
                                control={control}
                                name="MinimumRevisionAgeToKeep"
                                placeholder="Days"
                            />
                        </InputGroup>
                    )}
                    {(formValues.IsMinimumRevisionsToKeepEnabled || formValues.IsMinimumRevisionAgeToKeepEnabled) && (
                        <>
                            <FormSwitch control={control} name="IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled">
                                Set # of revisions to delete upon document update
                            </FormSwitch>
                            {formValues.IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled && (
                                <InputGroup className="mb-2">
                                    <FormInput
                                        type="number"
                                        control={control}
                                        name="MaximumRevisionsToDeleteUponDocumentUpdate"
                                        placeholder="Enter max revisions to delete (suggested 100)"
                                    />
                                </InputGroup>
                            )}
                        </>
                    )}

                    <Alert color="info" className="mt-3">
                        <ul className="m-0 p-0 vstack gap-1">
                            <li>
                                A revision will be created anytime a document is modified
                                {!formValues.IsPurgeOnDeleteEnabled && <span> or deleted</span>}.
                            </li>
                            {formValues.IsPurgeOnDeleteEnabled ? (
                                <li>When a document is deleted all its revisions will be removed.</li>
                            ) : (
                                <li>Revisions of a deleted document can be accessed in the Revisions Bin view.</li>
                            )}
                            {formValues.IsMinimumRevisionsToKeepEnabled && formValues.MinimumRevisionsToKeep && (
                                <>
                                    <li>
                                        {formValues.IsMinimumRevisionAgeToKeepEnabled &&
                                        formValues.MinimumRevisionAgeToKeep ? (
                                            <>
                                                <span>At least</span>{" "}
                                                <strong>{formValues.MinimumRevisionsToKeep}</strong> of the latest
                                            </>
                                        ) : (
                                            <>
                                                <span>
                                                    Only the latest <strong>{formValues.MinimumRevisionsToKeep}</strong>
                                                </span>
                                            </>
                                        )}{" "}
                                        {formValues.MinimumRevisionsToKeep === 1 ? (
                                            <span>revision</span>
                                        ) : (
                                            <span>revisions</span>
                                        )}{" "}
                                        will be kept.
                                    </li>
                                    {formValues.IsMinimumRevisionAgeToKeepEnabled &&
                                    formValues.MinimumRevisionAgeToKeep ? (
                                        <li>
                                            Older revisions will be removed if they exceed{" "}
                                            <strong>{formValues.MinimumRevisionAgeToKeep}</strong> on next revision
                                            creation.
                                        </li>
                                    ) : (
                                        <li>Older revisions will be removed on next revision creation.</li>
                                    )}
                                </>
                            )}
                            {!formValues.IsMinimumRevisionsToKeepEnabled &&
                                formValues.IsMinimumRevisionAgeToKeepEnabled &&
                                formValues.MinimumRevisionAgeToKeep && (
                                    <li>
                                        Revisions that exceed <strong>{formValues.MinimumRevisionAgeToKeep}</strong>{" "}
                                        will be removed on next revision creation.
                                    </li>
                                )}
                            {formValues.IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled &&
                                formValues.MaximumRevisionsToDeleteUponDocumentUpdate && (
                                    <li>
                                        A maximum of{" "}
                                        <strong>{formValues.MaximumRevisionsToDeleteUponDocumentUpdate}</strong>{" "}
                                        revisions will be deleted each time a document is updated, until the defined
                                        &apos;# of revisions to keep&apos; limit is reached.
                                    </li>
                                )}
                        </ul>
                    </Alert>
                </ModalBody>
                <ModalFooter>
                    <Button type="button" color="secondary" onClick={toggle}>
                        Cancel
                    </Button>
                    <Button type="submit" color="success">
                        <Icon icon="plus" />
                        Add config
                    </Button>
                </ModalFooter>
            </Form>
        </Modal>
    );
}

function getTitle(taskType: EditRevisionTaskType, configType: EditRevisionConfigType): string {
    let suffix = "";

    switch (configType) {
        case "collectionSpecific":
            suffix = "collection specific configuration";
            break;
        case "defaultDocument":
            suffix = "default document revisions configuration";
            break;
        case "defaultConflicts":
            suffix = "default conflicts revisions configuration";
            break;
        default:
            assertUnreachable(configType);
    }

    return `${_.startCase(taskType)} ${suffix}`;
}

function getInitialValues(config: DocumentRevisionsConfig): EditDocumentRevisionsCollectionConfig {
    if (!config) {
        return {
            Disabled: false,
            CollectionName: null,
            IsPurgeOnDeleteEnabled: false,
            IsMinimumRevisionAgeToKeepEnabled: false,
            MinimumRevisionAgeToKeep: null,
            IsMinimumRevisionsToKeepEnabled: false,
            MinimumRevisionsToKeep: null,
            IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled: false,
            MaximumRevisionsToDeleteUponDocumentUpdate: null,
        };
    }

    return {
        Disabled: config.Disabled,
        CollectionName: config.Name,
        IsPurgeOnDeleteEnabled: config.PurgeOnDelete,
        IsMinimumRevisionAgeToKeepEnabled: config.MinimumRevisionAgeToKeep != null,
        MinimumRevisionAgeToKeep: config.MinimumRevisionAgeToKeep,
        IsMinimumRevisionsToKeepEnabled: config.MinimumRevisionsToKeep != null,
        MinimumRevisionsToKeep: config.MinimumRevisionsToKeep,
        IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled: config.MaximumRevisionsToDeleteUponDocumentUpdate != null,
        MaximumRevisionsToDeleteUponDocumentUpdate: config.MaximumRevisionsToDeleteUponDocumentUpdate,
    };
}

function mapToDocumentRevisionsConfig(
    formData: EditDocumentRevisionsCollectionConfig,
    configType: EditRevisionConfigType
): DocumentRevisionsConfig {
    let name: DocumentRevisionsConfigName = null;

    switch (configType) {
        case "collectionSpecific":
            name = formData.CollectionName;
            break;
        case "defaultDocument":
            name = "Document Defaults";
            break;
        case "defaultConflicts":
            name = "Conflicting Document Defaults";
            break;
        default:
            assertUnreachable(configType);
    }

    return {
        Name: name,
        Disabled: formData.Disabled,
        MaximumRevisionsToDeleteUponDocumentUpdate: formData.MaximumRevisionsToDeleteUponDocumentUpdate,
        MinimumRevisionAgeToKeep: formData.MinimumRevisionAgeToKeep,
        MinimumRevisionsToKeep: formData.MinimumRevisionsToKeep,
        PurgeOnDelete: formData.IsPurgeOnDeleteEnabled,
    };
}
