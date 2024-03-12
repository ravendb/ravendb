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
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { throttledUpdateLicenseLimitsUsage } from "components/common/shell/setup";
import useBoolean from "components/hooks/useBoolean";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import useId from "components/hooks/useId";
import { useServices } from "components/hooks/useServices";
import {
    CustomSorterFormData,
    customSorterYupResolver,
} from "components/pages/database/settings/customSorters/EditCustomSorterValidation";
import { useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import database from "models/resources/database";
import React from "react";
import { useState } from "react";
import { useAsyncCallback } from "react-async-hook";
import { useForm, useWatch, SubmitHandler } from "react-hook-form";
import { Form, UncontrolledTooltip, Button, Collapse, InputGroup, Label } from "reactstrap";

interface DatabaseCustomSortersListItemProps {
    initialSorter: CustomSorterFormData;
    serverWideSorterNames: string[];
    db: database;
    remove: () => void;
}

export default function DatabaseCustomSortersListItem(props: DatabaseCustomSortersListItemProps) {
    const { initialSorter, serverWideSorterNames, db, remove } = props;

    const form = useForm<CustomSorterFormData>({
        resolver: customSorterYupResolver,
        defaultValues: initialSorter,
    });
    const { control, formState, handleSubmit, reset, setValue } = form;
    const formValues = useWatch({ control });
    useDirtyFlag(formState.isDirty);

    const { databasesService } = useServices();

    const asyncDeleteSorter = useAsyncCallback((name: string) => databasesService.deleteCustomSorter(db, name), {
        onSuccess: () => {
            remove();
            throttledUpdateLicenseLimitsUsage();
        },
    });

    const onSave: SubmitHandler<CustomSorterFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await databasesService.saveCustomSorter(db, {
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

    const isNew = !formState.defaultValues.name;
    const { value: isEditMode, toggle: toggleIsEditMode } = useBoolean(isNew);
    const [nameToConfirmDelete, setNameToConfirmDelete] = useState<string>(null);

    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const tooltipId = useId("override-info");

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
                        <>
                            <UncontrolledTooltip target={tooltipId} placement="left">
                                Overrides server-wide sorter
                            </UncontrolledTooltip>
                            <Icon id={tooltipId} icon="info" color="info" />
                        </>
                    )}
                    <RichPanelActions>
                        {isEditMode ? (
                            isDatabaseAdmin ? (
                                <>
                                    <Button key="save" type="submit" color="success" disabled={formState.isSubmitting}>
                                        <Icon icon="save" /> Save changes
                                    </Button>
                                    <Button key="cancel" type="button" color="secondary" onClick={onDiscard}>
                                        <Icon icon="cancel" />
                                        Discard
                                    </Button>
                                </>
                            ) : (
                                <Button key="preview" onClick={toggleIsEditMode}>
                                    <Icon icon="preview-off" margin="m-0" />
                                </Button>
                            )
                        ) : (
                            <>
                                <Button key="edit" onClick={toggleIsEditMode}>
                                    <Icon icon={isDatabaseAdmin ? "edit" : "preview"} margin="m-0" />
                                </Button>
                                {isDatabaseAdmin && (
                                    <>
                                        {nameToConfirmDelete != null && (
                                            <DeleteCustomSorterConfirm
                                                name={nameToConfirmDelete}
                                                onConfirm={(name) => asyncDeleteSorter.execute(name)}
                                                toggle={() => setNameToConfirmDelete(null)}
                                            />
                                        )}
                                        <ButtonWithSpinner
                                            key="delete"
                                            color="danger"
                                            onClick={() => setNameToConfirmDelete(formValues.name)}
                                            icon="trash"
                                            isSpinning={asyncDeleteSorter.status === "loading"}
                                        />
                                    </>
                                )}
                            </>
                        )}
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
                                    placeholder="Enter a sorter name"
                                />
                            </InputGroup>
                        )}
                        <InputGroup className="vstack">
                            {isDatabaseAdmin && (
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
                                readOnly={!isDatabaseAdmin}
                            />
                        </InputGroup>
                    </RichPanelDetails>
                </Collapse>
            </Form>
        </RichPanel>
    );
}
