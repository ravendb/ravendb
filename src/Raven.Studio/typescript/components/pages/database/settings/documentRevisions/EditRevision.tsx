import { Icon } from "components/common/Icon";
import React from "react";
import { Button, Form, InputGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import { FormDurationPicker, FormInput, FormSelectCreatable, FormSwitch } from "components/common/Form";
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
} from "./store/documentRevisionsSlice";
import { documentRevisionsSelectors } from "./store/documentRevisionsSliceSelectors";
import useEditRevisionFormController from "./useEditRevisionFormController";
import IconName from "typings/server/icons";
import { useAppSelector } from "components/store";
import { SelectOption } from "components/common/select/Select";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import genUtils from "common/generalUtils";
import generalUtils from "common/generalUtils";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import moment from "moment";
import RichAlert from "components/common/RichAlert";

const revisionsDelta = 100;
const revisionsByAgeDelta = 604800; // 7 days
const minimumRevisionsToKeepDefaultValue = 1000;

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

    const revisionsToKeepLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfRevisionsToKeep"));
    const revisionsAgeInDaysLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfRevisionAgeToKeepInDays"));

    const originalConfig = useAppSelector(documentRevisionsSelectors.originalConfig(config?.Name));
    const collectionConfigsNames = useAppSelector(documentRevisionsSelectors.allConfigsNames);
    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const newCollectionOptions: SelectOption[] = allCollectionNames
        .filter((name) => !collectionConfigsNames.includes(name))
        .map((name) => ({
            label: name,
            value: name,
        }));

    const { control, formState, setValue, handleSubmit } = useForm<EditDocumentRevisionsCollectionConfig>({
        resolver: isForNewCollection
            ? documentRevisionsCollectionConfigYupResolver
            : documentRevisionsConfigYupResolver,
        mode: "all",
        defaultValues: getInitialValues(config, revisionsToKeepLimit),
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

    const isRevisionsToKeepLimitTooLowWarning =
        originalConfig?.MinimumRevisionsToKeep != null &&
        formValues.minimumRevisionsToKeep != null &&
        !formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled &&
        originalConfig.MinimumRevisionsToKeep - formValues.minimumRevisionsToKeep > revisionsDelta;

    const isRevisionsToKeepLimitNotSetWarning =
        !formValues.isMinimumRevisionsToKeepEnabled || !formValues.minimumRevisionsToKeep;

    const isRevisionsToKeepByAgeLimitWarning =
        originalConfig?.MinimumRevisionAgeToKeep != null &&
        formValues.minimumRevisionAgeToKeep != null &&
        !formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled &&
        genUtils.timeSpanToSeconds(originalConfig.MinimumRevisionAgeToKeep) - formValues.minimumRevisionAgeToKeep >
            revisionsByAgeDelta;

    const isDefaultConflicts = config?.Name === documentRevisionsConfigNames.defaultConflicts;

    const minimumRevisionAgeToKeepDays = moment.duration(formValues.minimumRevisionAgeToKeep, "seconds").asDays();

    const isLimitExceeded =
        !isDefaultConflicts &&
        ((revisionsAgeInDaysLimit > 0 && minimumRevisionAgeToKeepDays > revisionsAgeInDaysLimit) ||
            (revisionsToKeepLimit > 0 && formValues.minimumRevisionsToKeep > revisionsToKeepLimit));

    return (
        <Modal isOpen toggle={toggle} wrapClassName="bs5" contentClassName="modal-border bulge-info" centered>
            <Form onSubmit={handleSubmit(onSubmit)} autoComplete="off">
                <ModalBody className="vstack gap-3">
                    <h4>{getTitle(taskType, configType)}</h4>
                    {configType === "collectionSpecific" && (
                        <InputGroup className="gap-1 flex-wrap flex-column">
                            <Label className="mb-0 md-label">Collection</Label>
                            <FormSelectCreatable
                                placeholder="Select collection (or enter new collection)"
                                control={control}
                                name="collectionName"
                                options={
                                    isForNewCollection
                                        ? newCollectionOptions
                                        : [{ label: config.Name, value: config.Name }]
                                }
                                isDisabled={!isForNewCollection}
                                maxMenuHeight={300}
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
                        <div>
                            <InputGroup className="mb-2">
                                <FormInput
                                    type="number"
                                    control={control}
                                    name="minimumRevisionsToKeep"
                                    placeholder="Enter number of revisions to keep"
                                />
                                {isRevisionsToKeepLimitTooLowWarning && <LimitTooLowWarning limit={revisionsDelta} />}
                            </InputGroup>
                            {!isDefaultConflicts &&
                            revisionsToKeepLimit > 0 &&
                            formValues.minimumRevisionsToKeep > revisionsToKeepLimit ? (
                                <RichAlert variant="warning" className="mb-2">
                                    Your license allows max {revisionsToKeepLimit} revisions to keep
                                </RichAlert>
                            ) : null}
                        </div>
                    )}
                    <FormSwitch control={control} name="isMinimumRevisionAgeToKeepEnabled">
                        Limit # of revisions to keep by age
                    </FormSwitch>
                    {formValues.isMinimumRevisionAgeToKeepEnabled && (
                        <div className="mb-2">
                            <FormDurationPicker
                                control={control}
                                name="minimumRevisionAgeToKeep"
                                showDays
                                showSeconds
                            />
                            {!isDefaultConflicts &&
                            revisionsAgeInDaysLimit > 0 &&
                            minimumRevisionAgeToKeepDays > revisionsAgeInDaysLimit ? (
                                <RichAlert variant="warning" className="my-2">
                                    Your license allows max {revisionsAgeInDaysLimit} days retention time
                                </RichAlert>
                            ) : null}
                            {isRevisionsToKeepByAgeLimitWarning && (
                                <LimitTooLowWarning
                                    limit={generalUtils.formatTimeSpan(revisionsByAgeDelta * 1000, true)}
                                />
                            )}
                        </div>
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
                    {isRevisionsToKeepLimitNotSetWarning && <LimitNotSetWarning />}
                    <RichAlert variant="primary" title="Summary" className="mt-2">
                        <ul className="m-0 ps-2 vstack gap-1">
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
                    </RichAlert>
                </ModalBody>
                <ModalFooter>
                    <Button type="button" color="link" className="link-muted" onClick={toggle}>
                        Cancel
                    </Button>
                    <Button type="submit" color="success" disabled={isLimitExceeded} title="Add this configuration">
                        <Icon icon={getSubmitIcon(taskType)} />
                        {_.startCase(taskType)} config
                    </Button>
                </ModalFooter>
            </Form>
        </Modal>
    );
}

interface LimitTooLowWarningProps {
    limit: number | string;
}

function LimitTooLowWarning({ limit }: LimitTooLowWarningProps) {
    return (
        <RichAlert variant="warning" className="mt-1">
            <div>
                The new limit is much lower than the current value (delta &gt; {limit}).
                <br />
                It is advised to set the # of revisions to delete upon document update.
            </div>
        </RichAlert>
    );
}

function LimitNotSetWarning() {
    return (
        <RichAlert variant="warning" className="mt-1">
            <div>
                No limit has been set on the number of revisions to keep.
                <br />
                An excessive number of revisions will lead to increased storage usage and may affect system performance.
            </div>
        </RichAlert>
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

function getInitialValues(
    config: DocumentRevisionsConfig,
    licenseRevisionsToKeepLimit: number
): EditDocumentRevisionsCollectionConfig {
    if (!config) {
        const minimumRevisionsToKeep =
            licenseRevisionsToKeepLimit && licenseRevisionsToKeepLimit < minimumRevisionsToKeepDefaultValue
                ? licenseRevisionsToKeepLimit
                : minimumRevisionsToKeepDefaultValue;

        return {
            disabled: false,
            collectionName: null,
            isPurgeOnDeleteEnabled: false,
            isMinimumRevisionAgeToKeepEnabled: false,
            minimumRevisionAgeToKeep: null,
            isMinimumRevisionsToKeepEnabled: true,
            minimumRevisionsToKeep,
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
        MaximumRevisionsToDeleteUponDocumentUpdate: formData.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled
            ? formData.maximumRevisionsToDeleteUponDocumentUpdate
            : null,
        MinimumRevisionAgeToKeep: formData.isMinimumRevisionAgeToKeepEnabled
            ? genUtils.formatAsTimeSpan(formData.minimumRevisionAgeToKeep * 1000)
            : null,
        MinimumRevisionsToKeep: formData.isMinimumRevisionsToKeepEnabled ? formData.minimumRevisionsToKeep : null,
        PurgeOnDelete: formData.isPurgeOnDeleteEnabled,
    };
}
