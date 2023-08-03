import { Icon } from "components/common/Icon";
import React, { useEffect } from "react";
import { Alert, Button, InputGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import { FormInput, FormSelect, FormSwitch } from "components/common/Form";
import { useForm, useWatch } from "react-hook-form";
import {
    DocumentRevisionsFormData,
    documentRevisionsYupResolver,
} from "components/pages/database/settings/documentRevisions/DocumentRevisionsValidation";
import { useDirtyFlag } from "hooks/useDirtyFlag";

interface EditRevisionProps {
    isOpen: boolean;
    toggle: () => void;
    onConfirm: () => Promise<void>;
    configType: "defaultDocument" | "collectionSpecific";
    taskType: "edit" | "new";
}

export default function EditRevision(props: EditRevisionProps) {
    const { isOpen, toggle, configType, taskType } = props;
    const { control, formState, reset, setValue } = useForm<DocumentRevisionsFormData>({
        resolver: documentRevisionsYupResolver,
        mode: "all",
    });

    const formValues = useWatch({ control: control });

    const collectionOptions = [
        { value: "option1", label: "Option 1" },
        { value: "option2", label: "Option 2" },
        { value: "option3", label: "Option 3" },
    ];

    useDirtyFlag(formState.isDirty);

    useEffect(() => {
        if (!formValues.PurgeOnDelete && formValues.PurgeOnDelete !== null) {
            setValue("PurgeOnDelete", false, { shouldValidate: true });
        }
    }, [setValue]);

    useEffect(() => {
        if (!formValues.IsMinimumRevisionsToKeepEnabled && !formValues.IsMinimumRevisionAgeToKeepEnabled) {
            setValue("IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled", false);
        }
    }, [formValues.IsMinimumRevisionsToKeepEnabled, formValues.IsMinimumRevisionAgeToKeepEnabled, setValue]);

    return (
        <Modal isOpen={isOpen} toggle={toggle} wrapClassName="bs5" contentClassName="modal-border bulge-info">
            <ModalBody className="vstack gap-2">
                {(taskType === "edit" && (
                    <h4>
                        Edit{" "}
                        {configType === "defaultDocument" ? (
                            <span>default document revisions</span>
                        ) : configType === "collectionSpecific" ? (
                            <span>collection specific configuration</span>
                        ) : null}
                    </h4>
                )) ||
                    (taskType === "new" && (
                        <h4>
                            Add{" "}
                            {configType === "defaultDocument" ? (
                                <span>default document revisions</span>
                            ) : configType === "collectionSpecific" ? (
                                <span>collection specific configuration</span>
                            ) : null}
                        </h4>
                    ))}
                {configType === "collectionSpecific" && (
                    <InputGroup className="gap-1 flex-wrap flex-column">
                        <Label className="mb-0 md-label">Collection</Label>
                        <FormSelect control={control} name="CollectionSpecificName" options={collectionOptions} />
                    </InputGroup>
                )}
                <FormSwitch control={control} name="PurgeOnDelete">
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
                        <FormInput type="text" control={control} name="MinimumRevisionAgeToKeep" placeholder="Days" />
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
                            {!formValues.PurgeOnDelete && <span> or deleted</span>}.
                        </li>
                        {formValues.PurgeOnDelete ? (
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
                                            <span>At least</span> <strong>{formValues.MinimumRevisionsToKeep}</strong>{" "}
                                            of the latest
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
                                {formValues.IsMinimumRevisionAgeToKeepEnabled && formValues.MinimumRevisionAgeToKeep ? (
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
                                    Revisions that exceed <strong>{formValues.MinimumRevisionAgeToKeep}</strong> will be
                                    removed on next revision creation.
                                </li>
                            )}
                        {formValues.IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled &&
                            formValues.MaximumRevisionsToDeleteUponDocumentUpdate && (
                                <li>
                                    A maximum of{" "}
                                    <strong>{formValues.MaximumRevisionsToDeleteUponDocumentUpdate}</strong> revisions
                                    will be deleted each time a document is updated, until the defined '# of revisions
                                    to keep' limit is reached.
                                </li>
                            )}
                    </ul>
                </Alert>
            </ModalBody>
            <ModalFooter>
                <Button color="secondary" onClick={toggle}>
                    Cancel
                </Button>
                <Button color="success">
                    <Icon icon="plus" />
                    Add config
                </Button>
            </ModalFooter>
        </Modal>
    );
}
