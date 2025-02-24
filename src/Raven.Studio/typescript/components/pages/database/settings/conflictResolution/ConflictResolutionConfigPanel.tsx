import React from "react";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
    RichPanelDetailItem,
} from "components/common/RichPanel";
import { Collapse, Form, InputGroup, Label } from "reactstrap";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { EditConflictResolutionSyntaxModal } from "components/pages/database/settings/conflictResolution/EditConflictResolutionSyntaxModal";
import { useAppDispatch, useAppSelector } from "components/store";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import useBoolean from "hooks/useBoolean";
import useUniqueId from "components/hooks/useUniqueId";
import genUtils from "common/generalUtils";
import {
    ConflictResolutionCollectionConfig,
    conflictResolutionActions,
    conflictResolutionSelectors,
} from "./store/conflictResolutionSlice";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import * as yup from "yup";
import { FormAceEditor, FormSelectCreatable } from "components/common/Form";
import { yupResolver } from "@hookform/resolvers/yup";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";

interface ConflictResolutionConfigPanelProps {
    initialConfig: ConflictResolutionCollectionConfig;
}

export default function ConflictResolutionConfigPanel({ initialConfig }: ConflictResolutionConfigPanelProps) {
    const dispatch = useAppDispatch();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames).filter(
        (x) => x !== "@empty" && x !== "@hilo"
    );
    const usedCollectionNames = useAppSelector(conflictResolutionSelectors.usedCollectionNames);
    const collectionOptions = allCollectionNames
        .filter((x) => !usedCollectionNames.includes(x))
        .map((x) => ({ label: x, value: x }));

    const { control, handleSubmit, reset } = useForm<FormData>({
        defaultValues: {
            collectionName: initialConfig.name,
            script: initialConfig.script,
        },
        resolver: yupResolver(getSchema(initialConfig.name, usedCollectionNames)),
    });

    const formValues = useWatch({ control });

    const { value: isSyntaxModalOpen, toggle: toggleIsSyntaxModalOpen } = useBoolean(false);

    const scriptPanelId = useUniqueId("scriptPanel");
    const unsavedChangesId = useUniqueId("unsavedChanges");
    const configId = initialConfig.id;

    const save: SubmitHandler<FormData> = (formData) => {
        dispatch(
            conflictResolutionActions.editSaved({
                id: configId,
                newConfig: {
                    name: formData.collectionName,
                    script: formData.script,
                },
            })
        );
    };

    return (
        <RichPanel className="flex-row" id={scriptPanelId}>
            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>
                            {formValues.collectionName || "Collection name"}
                            {(initialConfig.isEdited || initialConfig.isNewUnsaved) && (
                                <OverlayTrigger
                                    overlay={<Tooltip id={unsavedChangesId}>The script has not been saved yet</Tooltip>}
                                >
                                    <span id={unsavedChangesId} className="text-warning d-inline-block">
                                        *
                                    </span>
                                </OverlayTrigger>
                            )}
                        </RichPanelName>
                    </RichPanelInfo>
                    <Form onSubmit={handleSubmit(save)}>
                        <RichPanelActions>
                            <PanelActions
                                reset={reset}
                                isInEditMode={initialConfig.isInEditMode}
                                isNewUnsaved={initialConfig.isNewUnsaved}
                                configId={configId}
                            />
                        </RichPanelActions>
                    </Form>
                </RichPanelHeader>
                <Collapse isOpen={!initialConfig.isInEditMode}>
                    <RichPanelDetails>
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="clock" />
                                    Last modified
                                </>
                            }
                        >
                            {initialConfig.lastModifiedTime
                                ? genUtils.formatUtcDateAsLocal(initialConfig.lastModifiedTime)
                                : "(new)"}
                        </RichPanelDetailItem>
                    </RichPanelDetails>
                </Collapse>
                <Collapse isOpen={initialConfig.isInEditMode}>
                    <RichPanelDetails className="vstack gap-3 p-3">
                        {!initialConfig.name && (
                            <InputGroup className="vstack mb-1">
                                <Label>Collection</Label>
                                <FormSelectCreatable
                                    control={control}
                                    name="collectionName"
                                    placeholder="Select collection (or enter a new one)"
                                    options={collectionOptions}
                                    isClearable={false}
                                    maxMenuHeight={300}
                                    isDisabled={!hasDatabaseAdminAccess}
                                />
                            </InputGroup>
                        )}
                        <InputGroup className="vstack">
                            <Label className="d-flex flex-wrap justify-content-between">
                                Script
                                <Button
                                    variant="link"
                                    size="xs"
                                    onClick={toggleIsSyntaxModalOpen}
                                    className="p-0 align-self-end"
                                >
                                    Syntax
                                    <Icon icon="help" margin="ms-1" />
                                </Button>
                            </Label>
                            {isSyntaxModalOpen && (
                                <EditConflictResolutionSyntaxModal toggle={toggleIsSyntaxModalOpen} />
                            )}
                            <FormAceEditor
                                control={control}
                                name="script"
                                mode="javascript"
                                height="400px"
                                readOnly={!hasDatabaseAdminAccess}
                            />
                        </InputGroup>
                    </RichPanelDetails>
                </Collapse>
            </div>
        </RichPanel>
    );
}

function PanelActions({
    isInEditMode,
    isNewUnsaved,
    configId,
    reset,
}: {
    isInEditMode: boolean;
    isNewUnsaved: boolean;
    configId: string;
    reset: () => void;
}) {
    const dispatch = useAppDispatch();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const discard = () => {
        dispatch(conflictResolutionActions.editDiscarded(configId));
        reset();
    };

    if (hasDatabaseAdminAccess) {
        if (isInEditMode) {
            return (
                <React.Fragment key="actions-in-edit">
                    <Button type="submit" variant="success" title={isNewUnsaved ? "Add Script" : "Update Script"}>
                        <Icon icon="tick" margin="m-0" /> {isNewUnsaved ? "Add" : "Update"}
                    </Button>
                    <Button type="button" variant="secondary" title="Discard changes" onClick={discard}>
                        <Icon icon="cancel" margin="m-0" /> Discard
                    </Button>
                </React.Fragment>
            );
        } else {
            return (
                <React.Fragment key="actions-not-in-edit">
                    <Button
                        type="button"
                        variant="secondary"
                        title="Edit this script"
                        onClick={() => dispatch(conflictResolutionActions.edited(configId))}
                    >
                        <Icon icon="edit" margin="m-0" />
                    </Button>
                    <Button
                        type="button"
                        variant="danger"
                        title="Delete this script"
                        onClick={() => dispatch(conflictResolutionActions.deleted(configId))}
                    >
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </React.Fragment>
            );
        }
    }

    if (isInEditMode) {
        return (
            <Button
                type="button"
                variant="secondary"
                title="Hide this script"
                onClick={() => dispatch(conflictResolutionActions.editDiscarded(configId))}
            >
                <Icon icon="preview-off" margin="m-0" />
            </Button>
        );
    } else {
        return (
            <Button
                type="button"
                variant="secondary"
                title="Show this script"
                onClick={() => dispatch(conflictResolutionActions.edited(configId))}
            >
                <Icon icon="preview" margin="m-0" />
            </Button>
        );
    }
}

function getSchema(initialName: string, usedCollectionNames: string[]) {
    return yup.object({
        collectionName: yup
            .string()
            .required()
            .notOneOf(
                usedCollectionNames.filter((x) => x !== initialName),
                "This collection name is already used"
            ),
        script: yup.string().required(),
    });
}

type FormData = yup.InferType<ReturnType<typeof getSchema>>;
