import fileImporter from "common/fileImporter";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput, FormAceEditor } from "components/common/Form";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
} from "components/common/RichPanel";
import DeleteCustomSorterConfirm from "components/common/customSorters/DeleteCustomSorterConfirm";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { throttledUpdateLicenseLimitsUsage } from "components/common/shell/setup";
import useBoolean from "components/hooks/useBoolean";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import useUniqueId from "components/hooks/useUniqueId";
import { useServices } from "components/hooks/useServices";
import {
    CustomSorterFormData,
    customSorterYupResolver,
} from "components/common/customSorters/editCustomSorterValidation";
import { useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import React from "react";
import { useState } from "react";
import { UseAsyncReturn, useAsyncCallback } from "react-async-hook";
import { useForm, useWatch, SubmitHandler } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";
import Label from "components/common/Label";
import DatabaseCustomSorterTest from "components/pages/database/settings/customSorters/DatabaseCustomSorterTest";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import Button from "react-bootstrap/Button";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";

interface DatabaseCustomSortersListItemProps {
    initialSorter: CustomSorterFormData;
    serverWideSorterNames: string[];
    remove: () => void;
}

export default function DatabaseCustomSortersListItem(props: DatabaseCustomSortersListItemProps) {
    const { initialSorter, serverWideSorterNames, remove } = props;

    const form = useForm<CustomSorterFormData>({
        resolver: customSorterYupResolver,
        defaultValues: initialSorter,
    });
    const { control, formState, handleSubmit, reset, setValue } = form;
    const formValues = useWatch({ control });
    useDirtyFlag(formState.isDirty);

    const isNew = !formState.defaultValues.name;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { value: isEditMode, toggle: toggleIsEditMode } = useBoolean(isNew);
    const { value: isTestMode, toggle: toggleIsTestMode } = useBoolean(false);

    const [nameToConfirmDelete, setNameToConfirmDelete] = useState<string>(null);

    const tooltipId = useUniqueId("override-info");

    const { databasesService } = useServices();

    const asyncDeleteSorter = useAsyncCallback(
        (sorterName: string) => databasesService.deleteCustomSorter(databaseName, sorterName),
        {
            onSuccess: () => {
                remove();
                throttledUpdateLicenseLimitsUsage();
            },
        }
    );

    const onSave: SubmitHandler<CustomSorterFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await databasesService.saveCustomSorter(databaseName, {
                Name: formData.name,
                Code: formData.code,
            });

            toggleIsEditMode();
            reset(formData);
            throttledUpdateLicenseLimitsUsage();
        });
    };

    const onDiscard = () => {
        toggleIsEditMode();
        if (isNew) {
            remove();
        }
    };

    return (
        <RichPanel className="mt-3">
            <Form onSubmit={handleSubmit(onSave)}>
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>
                            {formValues.name || "New sorter name"}
                            {(formState.isDirty || isNew) && <span className="text-warning ms-1">*</span>}
                        </RichPanelName>
                    </RichPanelInfo>
                    {serverWideSorterNames.includes(formValues.name) && (
                        <OverlayTrigger overlay={<Tooltip id={tooltipId}>Overrides server-wide sorter</Tooltip>}>
                            <div className="d-inline-block">
                                <Icon id={tooltipId} icon="info" color="info" />
                            </div>
                        </OverlayTrigger>
                    )}
                    <RichPanelActions>
                        <CustomSortersActions
                            name={formValues.name}
                            isTestMode={isTestMode}
                            toggleIsTestMode={toggleIsTestMode}
                            isEditMode={isEditMode}
                            toggleIsEditMode={toggleIsEditMode}
                            onDiscard={onDiscard}
                            nameToConfirmDelete={nameToConfirmDelete}
                            setNameToConfirmDelete={setNameToConfirmDelete}
                            isSubmitting={formState.isSubmitting}
                            asyncDeleteSorter={asyncDeleteSorter}
                        />
                    </RichPanelActions>
                </RichPanelHeader>

                <Collapse in={isTestMode}>
                    <div>
                        <DatabaseCustomSorterTest name={formValues.name} />
                    </div>
                </Collapse>

                <Collapse in={isEditMode}>
                    <div>
                        <RichPanelDetails className="vstack gap-3 p-4">
                            {isNew && (
                                <InputGroup className="vstack mb-1">
                                    <Label>Name</Label>
                                    <FormInput
                                        type="text"
                                        control={control}
                                        name="name"
                                        placeholder="Enter a sorter name"
                                    />
                                </InputGroup>
                            )}
                            <InputGroup className="vstack">
                                {hasDatabaseAdminAccess && (
                                    <div className="d-flex justify-content-end">
                                        <Label className="btn btn-link btn-xs text-right">
                                            <Icon icon="upload" />
                                            Load from a file
                                            <input
                                                type="file"
                                                className="d-none"
                                                onChange={(e) =>
                                                    fileImporter.readAsBinaryString(e.currentTarget, (x) =>
                                                        setValue("code", x)
                                                    )
                                                }
                                                accept=".cs"
                                            />
                                        </Label>
                                    </div>
                                )}
                                <FormAceEditor
                                    control={control}
                                    name="code"
                                    mode="csharp"
                                    height="400px"
                                    readOnly={!hasDatabaseAdminAccess}
                                />
                            </InputGroup>
                        </RichPanelDetails>
                    </div>
                </Collapse>
            </Form>
        </RichPanel>
    );
}

interface CustomSortersActionsProps {
    toggleIsTestMode: () => void;
    isEditMode: boolean;
    isTestMode: boolean;
    onDiscard: () => void;
    toggleIsEditMode: () => void;
    nameToConfirmDelete: string;
    name: string;
    setNameToConfirmDelete: (name: string) => void;
    isSubmitting: boolean;
    asyncDeleteSorter: UseAsyncReturn<void, [name: string]>;
}

function CustomSortersActions({
    toggleIsTestMode,
    isEditMode,
    isTestMode,
    onDiscard,
    toggleIsEditMode,
    nameToConfirmDelete,
    name,
    setNameToConfirmDelete,
    isSubmitting,
    asyncDeleteSorter,
}: CustomSortersActionsProps) {
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    if (!hasDatabaseAdminAccess) {
        return isEditMode ? (
            <Button variant="secondary" key="preview" onClick={toggleIsEditMode}>
                <Icon icon="preview-off" margin="m-0" />
            </Button>
        ) : (
            <Button variant="secondary" key="edit" onClick={toggleIsEditMode} disabled={isTestMode}>
                <Icon icon="preview" margin="m-0" />
            </Button>
        );
    }

    return (
        <>
            <ConditionalPopover
                conditions={{
                    isActive: isEditMode,
                    message: "To test, first exit edit mode",
                }}
            >
                <Button variant="secondary" key="test" onClick={toggleIsTestMode} disabled={isEditMode}>
                    <Icon icon="rocket" addon={isTestMode ? "cancel" : null} margin="m-0" />
                </Button>
            </ConditionalPopover>

            {isEditMode ? (
                <>
                    <Button key="save" type="submit" variant="success" disabled={isSubmitting}>
                        <Icon icon="save" /> Save changes
                    </Button>
                    <Button key="cancel" type="button" variant="secondary" onClick={onDiscard}>
                        <Icon icon="cancel" />
                        Discard
                    </Button>
                </>
            ) : (
                <>
                    <ConditionalPopover
                        conditions={{
                            isActive: isTestMode,
                            message: "To edit, first exit test mode",
                        }}
                    >
                        <Button variant="secondary" key="edit" onClick={toggleIsEditMode} disabled={isTestMode}>
                            <Icon icon={hasDatabaseAdminAccess ? "edit" : "preview"} margin="m-0" />
                        </Button>
                    </ConditionalPopover>
                    {hasDatabaseAdminAccess && (
                        <>
                            {nameToConfirmDelete != null && (
                                <DeleteCustomSorterConfirm
                                    name={nameToConfirmDelete}
                                    onConfirm={asyncDeleteSorter.execute}
                                    toggle={() => setNameToConfirmDelete(null)}
                                />
                            )}
                            <ButtonWithSpinner
                                key="delete"
                                variant="danger"
                                onClick={() => setNameToConfirmDelete(name)}
                                icon="trash"
                                isSpinning={asyncDeleteSorter.status === "loading"}
                            />
                        </>
                    )}
                </>
            )}
        </>
    );
}
