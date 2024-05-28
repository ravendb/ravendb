import {
    CustomAnalyzerFormData,
    customAnalyzerYupResolver,
} from "components/common/customAnalyzers/editCustomAnalyzerValidation";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import { useAsyncCallback } from "react-async-hook";
import { throttledUpdateLicenseLimitsUsage } from "components/common/shell/setup";
import { useServices } from "hooks/useServices";
import { tryHandleSubmit } from "components/utils/common";
import useBoolean from "hooks/useBoolean";
import React, { useState } from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import { Button, Collapse, Form, InputGroup, Label } from "reactstrap";
import { Icon } from "components/common/Icon";
import DeleteCustomAnalyzerConfirm from "components/common/customAnalyzers/DeleteCustomAnalyzerConfirm";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormAceEditor, FormInput } from "components/common/Form";
import fileImporter from "common/fileImporter";

interface ServerWideCustomAnalyzersListItemProps {
    initialAnalyzer: CustomAnalyzerFormData;
    remove: () => void;
}

export default function ServerWideCustomAnalyzersListItem(props: ServerWideCustomAnalyzersListItemProps) {
    const { initialAnalyzer, remove } = props;

    const form = useForm<CustomAnalyzerFormData>({
        resolver: customAnalyzerYupResolver,
        defaultValues: initialAnalyzer,
    });

    const { control, formState, handleSubmit, reset, setValue } = form;
    const formValues = useWatch({ control });
    useDirtyFlag(formState.isDirty);

    const { manageServerService } = useServices();

    const asyncDeleteAnalyzer = useAsyncCallback(
        (name: string) => manageServerService.deleteServerWideCustomAnalyzer(name),
        {
            onSuccess: () => {
                remove();
                throttledUpdateLicenseLimitsUsage();
            },
        }
    );

    const onSave: SubmitHandler<CustomAnalyzerFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveServerWideCustomAnalyzer({
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
                            {formValues.name || "New analyzer name"}
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
                                    <DeleteCustomAnalyzerConfirm
                                        name={nameToConfirmDelete}
                                        toggle={() => setNameToConfirmDelete(null)}
                                        onConfirm={(name) => asyncDeleteAnalyzer.execute(name)}
                                    />
                                )}
                                <ButtonWithSpinner
                                    key="delete"
                                    color="danger"
                                    onClick={() => setNameToConfirmDelete(formValues.name)}
                                    icon="trash"
                                    isSpinning={asyncDeleteAnalyzer.status === "loading"}
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
                                    placeholder="Enter analyzer name"
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
                            <FormAceEditor name="code" control={control} mode="csharp" height="400px" />
                        </InputGroup>
                    </RichPanelDetails>
                </Collapse>
            </Form>
        </RichPanel>
    );
}
