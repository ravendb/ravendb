import { Icon } from "components/common/Icon";
import React from "react";
import { Alert, Button, Form, InputGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import { FormDurationPicker, FormInput, FormSelect, FormSwitch } from "components/common/Form";
import { SubmitHandler, useForm } from "react-hook-form";
import {
    EditDocumentRevisionsCollectionConfig,
    documentRevisionsCollectionConfigYupResolver,
    documentRevisionsConfigYupResolver,
} from "components/pages/database/settings/documentRevisions/DocumentRevisionsValidation";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import assertUnreachable from "components/utils/assertUnreachable";
import {
    DocumentRevisionsConfig,
    DocumentRevisionsConfigName,
    documentRevisionsConfigNames,
    documentRevisionsSelectors,
} from "./store/documentRevisionsSlice";
import useEditRevisionFormController from "./useEditRevisionFormController";
import IconName from "typings/server/icons";
import { useAppSelector } from "components/store";
import { SelectOption } from "components/common/Select";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import genUtils from "common/generalUtils";
import generalUtils from "common/generalUtils";

const revisionsDelta = 100;
const revisionsByAgeDelta = 604800; // 7 days

export type EditRevisionConfigType = "collectionSpecific" | keyof typeof documentRevisionsConfigNames;
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

    const isForNewCollection: boolean = configType === "collectionSpecific" && taskType === "new";

    const originalConfig = useAppSelector(documentRevisionsSelectors.originalConfig(config?.Name));
    const collectionConfigsNames = useAppSelector(documentRevisionsSelectors.allConfigsNames);
    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const collectionOptions: SelectOption<string>[] = allCollectionNames
        .filter((name) => !collectionConfigsNames.includes(name))
        .map((name) => ({ label: name, value: name }));

    const { control, formState, setValue, handleSubmit } = useForm<EditDocumentRevisionsCollectionConfig>({
        resolver: isForNewCollection
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

    const formattedMinimumRevisionAgeToKeep = formValues.minimumRevisionAgeToKeep
        ? generalUtils.formatTimeSpan(formValues.minimumRevisionAgeToKeep * 1000, true)
        : null;

    const isRevisionsToKeepLimitWarning =
        originalConfig?.MinimumRevisionsToKeep &&
        formValues.minimumRevisionsToKeep &&
        !formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled &&
        originalConfig.MinimumRevisionsToKeep - formValues.minimumRevisionsToKeep > revisionsDelta;

    const isRevisionsToKeepByAgeLimitWarning =
        originalConfig?.MinimumRevisionAgeToKeep &&
        formValues.minimumRevisionAgeToKeep &&
        !formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled &&
        genUtils.timeSpanToSeconds(originalConfig.MinimumRevisionAgeToKeep) - formValues.minimumRevisionAgeToKeep >
            revisionsByAgeDelta;

    return (
        <Modal isOpen toggle={toggle} wrapClassName="bs5" contentClassName="modal-border bulge-info">
            <Form onSubmit={handleSubmit(onSubmit)} autoComplete="off">
                <ModalBody className="vstack gap-2">
                    <h4>{getTitle(taskType, configType)}</h4>
                    {configType === "collectionSpecific" && (
                        <InputGroup className="gap-1 flex-wrap flex-column">
                            <Label className="mb-0 md-label">Collection</Label>
                            <FormSelect
                                control={control}
                                name="collectionName"
                                options={collectionOptions}
                                disabled={!isForNewCollection}
                            />
                        </InputGroup>
                    )}
                    <FormSwitch control={control} name="isPurgeOnDeleteEnabled">
                        Purge revisions on document delete
                    </FormSwitch>
                    <FormSwitch control={control} name="isMinimumRevisionsToKeepEnabled">
                        Limit # of revisions to keep
                    </FormSwitch>
                    {formValues.isMinimumRevisionsToKeepEnabled && (
                        <InputGroup className="mb-2">
                            <FormInput
                                type="number"
                                control={control}
                                name="minimumRevisionsToKeep"
                                placeholder="Enter number of revisions to keep"
                            />
                            {isRevisionsToKeepLimitWarning && <LimitWarning limit={revisionsDelta} />}
                        </InputGroup>
                    )}
                    <FormSwitch control={control} name="isMinimumRevisionAgeToKeepEnabled">
                        Limit # of revisions to keep by age
                    </FormSwitch>
                    {formValues.isMinimumRevisionAgeToKeepEnabled && (
                        <InputGroup className="mb-2">
                            <FormDurationPicker
                                control={control}
                                name="minimumRevisionAgeToKeep"
                                showDays
                                showSeconds
                            />
                            {isRevisionsToKeepByAgeLimitWarning && (
                                <LimitWarning limit={generalUtils.formatTimeSpan(revisionsByAgeDelta * 1000, true)} />
                            )}
                        </InputGroup>
                    )}
                    {(formValues.isMinimumRevisionsToKeepEnabled || formValues.isMinimumRevisionAgeToKeepEnabled) && (
                        <>
                            <FormSwitch control={control} name="isMaximumRevisionsToDeleteUponDocumentUpdateEnabled">
                                Set # of revisions to delete upon document update
                            </FormSwitch>
                            {formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled && (
                                <InputGroup className="mb-2">
                                    <FormInput
                                        type="number"
                                        control={control}
                                        name="maximumRevisionsToDeleteUponDocumentUpdate"
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
                                {!formValues.isPurgeOnDeleteEnabled && <span> or deleted</span>}.
                            </li>
                            {formValues.isPurgeOnDeleteEnabled ? (
                                <li>When a document is deleted all its revisions will be removed.</li>
                            ) : (
                                <li>Revisions of a deleted document can be accessed in the Revisions Bin view.</li>
                            )}
                            {formValues.minimumRevisionsToKeep > 0 && (
                                <>
                                    <li>
                                        {formValues.minimumRevisionAgeToKeep > 0 ? (
                                            <>
                                                <span>At least</span>{" "}
                                                <strong>{formValues.minimumRevisionsToKeep}</strong> of the latest
                                            </>
                                        ) : (
                                            <span>
                                                Only the latest <strong>{formValues.minimumRevisionsToKeep}</strong>
                                            </span>
                                        )}
                                        <span>
                                            {formValues.minimumRevisionsToKeep === 1 ? " revision " : " revisions "}
                                        </span>
                                        will be kept.
                                    </li>
                                    {formValues.minimumRevisionAgeToKeep > 0 ? (
                                        <li>
                                            Older revisions will be removed if they exceed{" "}
                                            <strong>{formattedMinimumRevisionAgeToKeep}</strong> on next revision
                                            creation.
                                        </li>
                                    ) : (
                                        <li>Older revisions will be removed on next revision creation.</li>
                                    )}
                                </>
                            )}
                            {!formValues.minimumRevisionsToKeep && formValues.minimumRevisionAgeToKeep > 0 && (
                                <li>
                                    Revisions that exceed <strong>{formattedMinimumRevisionAgeToKeep}</strong> will be
                                    removed on next revision creation.
                                </li>
                            )}
                            {formValues.maximumRevisionsToDeleteUponDocumentUpdate > 0 && (
                                <li>
                                    A maximum of{" "}
                                    <strong>{formValues.maximumRevisionsToDeleteUponDocumentUpdate}</strong> revisions
                                    will be deleted each time a document is updated, until the defined &apos;# of
                                    revisions to keep&apos; limit is reached.
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
                        <Icon icon={getSubmitIcon(taskType)} />
                        {_.startCase(taskType)} config
                    </Button>
                </ModalFooter>
            </Form>
        </Modal>
    );
}

interface LimitWarningProps {
    limit: number | string;
}

function LimitWarning({ limit }: LimitWarningProps) {
    return (
        <Alert color="warning" className="mt-1">
            The new limit is much lower than the current value (delta &gt; {limit}).
            <br />
            It is advised to set the # of revisions to delete upon document update.
        </Alert>
    );
}

function getSubmitIcon(taskType: EditRevisionTaskType): IconName {
    switch (taskType) {
        case "new":
            return "plus";
        case "edit":
            return "edit";
        default:
            assertUnreachable(taskType);
    }
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
            disabled: false,
            collectionName: null,
            isPurgeOnDeleteEnabled: false,
            isMinimumRevisionAgeToKeepEnabled: false,
            minimumRevisionAgeToKeep: null,
            isMinimumRevisionsToKeepEnabled: false,
            minimumRevisionsToKeep: null,
            isMaximumRevisionsToDeleteUponDocumentUpdateEnabled: false,
            maximumRevisionsToDeleteUponDocumentUpdate: null,
        };
    }

    return {
        disabled: config.Disabled,
        collectionName: config.Name,
        isPurgeOnDeleteEnabled: config.PurgeOnDelete,
        isMinimumRevisionAgeToKeepEnabled: config.MinimumRevisionAgeToKeep != null,
        minimumRevisionAgeToKeep: genUtils.timeSpanToSeconds(config.MinimumRevisionAgeToKeep),
        isMinimumRevisionsToKeepEnabled: config.MinimumRevisionsToKeep != null,
        minimumRevisionsToKeep: config.MinimumRevisionsToKeep,
        isMaximumRevisionsToDeleteUponDocumentUpdateEnabled: config.MaximumRevisionsToDeleteUponDocumentUpdate != null,
        maximumRevisionsToDeleteUponDocumentUpdate: config.MaximumRevisionsToDeleteUponDocumentUpdate,
    };
}

function mapToDocumentRevisionsConfig(
    formData: EditDocumentRevisionsCollectionConfig,
    configType: EditRevisionConfigType
): DocumentRevisionsConfig {
    let name: DocumentRevisionsConfigName = null;

    switch (configType) {
        case "collectionSpecific":
            name = formData.collectionName;
            break;
        case "defaultDocument":
            name = documentRevisionsConfigNames.defaultDocument;
            break;
        case "defaultConflicts":
            name = documentRevisionsConfigNames.defaultConflicts;
            break;
        default:
            assertUnreachable(configType);
    }

    return {
        Name: name,
        Disabled: formData.disabled,
        MaximumRevisionsToDeleteUponDocumentUpdate: formData.maximumRevisionsToDeleteUponDocumentUpdate,
        MinimumRevisionAgeToKeep: genUtils.formatAsTimeSpan(formData.minimumRevisionAgeToKeep * 1000),
        MinimumRevisionsToKeep: formData.minimumRevisionsToKeep,
        PurgeOnDelete: formData.isPurgeOnDeleteEnabled,
    };
}
