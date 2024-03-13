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
import {
    CustomSorterFormData,
    customSorterYupResolver,
} from "components/common/customSorters/editCustomSorterValidation";
import { throttledUpdateLicenseLimitsUsage } from "components/common/shell/setup";
import useBoolean from "components/hooks/useBoolean";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useServices } from "components/hooks/useServices";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { useAsyncCallback } from "react-async-hook";
import { useForm, useWatch, SubmitHandler } from "react-hook-form";
import { Button, Collapse, Form, InputGroup, Label } from "reactstrap";

interface ServerWideCustomSortersListItemProps {
    initialSorter: CustomSorterFormData;
    remove: () => void;
}

export default function ServerWideCustomSortersListItem(props: ServerWideCustomSortersListItemProps) {
    const { initialSorter, remove } = props;

    const form = useForm<CustomSorterFormData>({
        resolver: customSorterYupResolver,
        defaultValues: initialSorter,
    });
    const { control, formState, handleSubmit, reset, setValue } = form;
    const formValues = useWatch({ control });
    useDirtyFlag(formState.isDirty);

    const { manageServerService } = useServices();

    const asyncDeleteSorter = useAsyncCallback(
        (name: string) => manageServerService.deleteServerWideCustomSorter(name),
        {
            onSuccess: () => {
                remove();
                throttledUpdateLicenseLimitsUsage();
            },
        }
    );

    const onSave: SubmitHandler<CustomSorterFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveServerWideCustomSorter({
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

                    <RichPanelActions>
                        {isEditMode ? (
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
                            <>
                                <Button key="edit" onClick={toggleIsEditMode}>
                                    <Icon icon="edit" margin="m-0" />
                                </Button>
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
                            <div className="d-flex justify-content-end">
                                <Label className="btn btn-link btn-xs text-right">
                                    <Icon icon="upload" />
                                    Load from a file
                                    <input
                                        type="file"
                                        className="d-none"
                                        onChange={(e) =>
                                            fileImporter.readAsBinaryString(e.currentTarget, (x) => setValue("code", x))
                                        }
                                        accept=".cs"
                                    />
                                </Label>
                            </div>
                            <FormAceEditor control={control} name="code" mode="csharp" height="400px" />
                        </InputGroup>
                    </RichPanelDetails>
                </Collapse>
            </Form>
        </RichPanel>
    );
}
