import React, { useState } from "react";
import {
    CustomAnalyzerFormData,
    customAnalyzerYupResolver,
} from "components/common/customAnalyzers/editCustomAnalyzerValidation";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "hooks/useBoolean";
import useId from "hooks/useId";
import { useServices } from "hooks/useServices";
import { useAsyncCallback, UseAsyncReturn } from "react-async-hook";
import { throttledUpdateLicenseLimitsUsage } from "components/common/shell/setup";
import { tryHandleSubmit } from "components/utils/common";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import { Button, Collapse, Form, InputGroup, Label, UncontrolledTooltip } from "reactstrap";
import { Icon } from "components/common/Icon";
import DeleteCustomAnalyzerConfirm from "components/common/customAnalyzers/DeleteCustomAnalyzerConfirm";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormAceEditor, FormInput } from "components/common/Form";
import fileImporter from "common/fileImporter";

interface DatabaseCustomAnalyzersListItemProps {
    initialAnalyzer: CustomAnalyzerFormData;
    serverWideAnalyzerNames: string[];
    remove: () => void;
}

export default function DatabaseCustomAnalyzersListItem(props: DatabaseCustomAnalyzersListItemProps) {
    const { initialAnalyzer, serverWideAnalyzerNames, remove } = props;
    const form = useForm<CustomAnalyzerFormData>({
        resolver: customAnalyzerYupResolver,
        defaultValues: initialAnalyzer,
    });
    const { control, formState, handleSubmit, reset, setValue } = form;
    const formValues = useWatch({ control });
    useDirtyFlag(formState.isDirty);

    const isNew = !formState.defaultValues.name;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { value: isEditMode, toggle: toggleIsEditMode } = useBoolean(isNew);

    const [nameToConfirmDelete, setNameToConfirmDelete] = useState<string>(null);

    const tooltipId = useId("override-info");

    const { databasesService } = useServices();

    const asyncDeleteAnalyzer = useAsyncCallback(
        (analyzerName: string) => databasesService.deleteCustomAnalyzer(databaseName, analyzerName),
        {
            onSuccess: () => {
                remove();
                throttledUpdateLicenseLimitsUsage();
            },
        }
    );

    const onSave: SubmitHandler<CustomAnalyzerFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await databasesService.saveCustomAnalyzer(databaseName, {
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
                            {formValues.name || "New analyzer name"}
                            {(formState.isDirty || isNew) && <span className="text-warning ms-1">*</span>}
                        </RichPanelName>
                    </RichPanelInfo>
                    {serverWideAnalyzerNames.includes(formValues.name) && (
                        <>
                            <UncontrolledTooltip target={tooltipId} placement="left">
                                Override server-wide analyzer
                            </UncontrolledTooltip>
                            <Icon id={tooltipId} icon="info" color="info" />
                        </>
                    )}
                    <RichPanelActions>
                        <CustomAnalyzersActions
                            name={formValues.name}
                            isEditMode={isEditMode}
                            toggleIsEditMode={toggleIsEditMode}
                            onDiscard={onDiscard}
                            nameToConfirmDelete={nameToConfirmDelete}
                            setNameToConfirmDelete={setNameToConfirmDelete}
                            isSubmitting={formState.isSubmitting}
                            asyncDeleteAnalyzer={asyncDeleteAnalyzer}
                        />
                    </RichPanelActions>
                </RichPanelHeader>

                <Collapse isOpen={isEditMode}>
                    <RichPanelDetails className="vstack gap-3 p-4">
                        {isNew && (
                            <InputGroup className="vstack mb-1">
                                <Label>Name</Label>
                                <FormInput
                                    type="text"
                                    control={control}
                                    name="name"
                                    placeholder="Enter analyzer name"
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
                </Collapse>
            </Form>
        </RichPanel>
    );
}

interface CustomAnalyzersActionsProps {
    isEditMode: boolean;
    onDiscard: () => void;
    toggleIsEditMode: () => void;
    nameToConfirmDelete: string;
    name: string;
    setNameToConfirmDelete: (name: string) => void;
    isSubmitting: boolean;
    asyncDeleteAnalyzer: UseAsyncReturn<void, [name: string]>;
}

function CustomAnalyzersActions({
    isEditMode,
    onDiscard,
    toggleIsEditMode,
    nameToConfirmDelete,
    name,
    setNameToConfirmDelete,
    isSubmitting,
    asyncDeleteAnalyzer,
}: CustomAnalyzersActionsProps) {
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    if (!hasDatabaseAdminAccess) {
        return isEditMode ? (
            <Button key="preview" onClick={toggleIsEditMode}>
                <Icon icon="preview-off" margin="m-0" />
            </Button>
        ) : (
            <Button key="edit" onClick={toggleIsEditMode}>
                <Icon icon="preview" margin="m-0" />
            </Button>
        );
    }

    return (
        <>
            {isEditMode ? (
                <>
                    <Button key="save" type="submit" color="success" disabled={isSubmitting}>
                        <Icon icon="save" /> Save changes
                    </Button>
                    <Button key="cancel" type="button" color="secondary" onClick={onDiscard}>
                        <Icon icon="cancel" /> Discard
                    </Button>
                </>
            ) : (
                <>
                    <Button key="edit" onClick={toggleIsEditMode}>
                        <Icon icon={hasDatabaseAdminAccess ? "edit" : "preview"} margin="m-0" />
                    </Button>
                    {hasDatabaseAdminAccess && (
                        <>
                            {nameToConfirmDelete != null && (
                                <DeleteCustomAnalyzerConfirm
                                    name={nameToConfirmDelete}
                                    toggle={() => setNameToConfirmDelete(null)}
                                    onConfirm={asyncDeleteAnalyzer.execute}
                                />
                            )}
                            <ButtonWithSpinner
                                key="delete"
                                color="danger"
                                onClick={() => setNameToConfirmDelete(name)}
                                icon="trash"
                                isSpinning={asyncDeleteAnalyzer.status === "loading"}
                            />
                        </>
                    )}
                </>
            )}
        </>
    );
}
