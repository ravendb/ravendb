import {
    RemoteAttachmentsDestinationFormData,
    remoteAttachmentsDestinationYupResolver,
    RemoteAttachmentsFormData,
} from "components/pages/database/settings/remoteAttachments/remoteAttachmentsValidation";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelStatus,
} from "components/common/RichPanel";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useServices } from "hooks/useServices";
import { useAppDispatch, useAppSelector } from "components/store";
import { remoteAttachmentsSelectors } from "components/pages/database/settings/remoteAttachments/store/remoteAttachmentsSliceSelectors";
import { FormProvider, SubmitHandler, useForm, useFormContext } from "react-hook-form";
import {
    defaultAzureFormData,
    defaultS3FormData,
} from "components/common/formDestinations/utils/formDestinationsMapsFromDto";
import { useAsyncCallback } from "react-async-hook";
import { mapAzureToDto, mapS3ToDto } from "components/common/formDestinations/utils/formDestinationsMapsToDto";
import { useEffect } from "react";
import { remoteAttachmentsActions } from "components/pages/database/settings/remoteAttachments/store/remoteAttachmentsSlice";
import { FormGroup, FormLabel, FormSelect } from "components/common/Form";
import {
    RemoteAttachmentsAzureFields,
    RemoteAttachmentsS3Fields,
} from "components/pages/database/settings/remoteAttachments/partials/RemoteAttachmentsFields";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

interface DestinationPanelProps extends RemoteAttachmentsDestinationFormData {
    onEdit: (id: string) => void;
    onDelete: (id: string) => void;
    onToggle: (id: string) => void;
}

export function DestinationPanel({
    provider,
    identifier,
    disabled,
    onEdit,
    onDelete,
    onToggle,
}: DestinationPanelProps) {
    const { watch } = useFormContext<RemoteAttachmentsFormData>();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const isModified = useAppSelector(remoteAttachmentsSelectors.isDestinationModified(identifier));
    const formValues = watch();
    return (
        <RichPanel className="with-status flex-row">
            <RichPanelStatus className={disabled ? "bg-danger" : "bg-success"}>
                {disabled ? "Disabled" : "Enabled"}
            </RichPanelStatus>
            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>
                            {identifier}
                            {isModified && <span className="text-warning ms-1">*</span>}
                        </RichPanelName>
                    </RichPanelInfo>
                    {hasDatabaseAdminAccess && (
                        <RichPanelActions>
                            <Button
                                disabled={!formValues.isRemoteAttachmentsEnabled}
                                variant={disabled ? "success" : "secondary"}
                                onClick={() => onToggle(identifier)}
                            >
                                <Icon icon={disabled ? "disable" : "play"} />
                                {disabled ? "Disable" : "Enable"}
                            </Button>
                            <Button
                                disabled={!formValues.isRemoteAttachmentsEnabled}
                                variant="secondary"
                                onClick={() => onEdit(identifier)}
                            >
                                <Icon icon="edit" margin="m-0" />
                            </Button>
                            <Button
                                disabled={!formValues.isRemoteAttachmentsEnabled}
                                variant="danger"
                                onClick={() => onDelete(identifier)}
                            >
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        </RichPanelActions>
                    )}
                </RichPanelHeader>
                <RichPanelDetails>
                    <RichPanelDetailItem size="sm" label="Destination">
                        <Icon icon={provider === "s3" ? "aws" : "azure"} />
                        {provider === "s3" ? "Amazon S3" : "Azure"}
                    </RichPanelDetailItem>
                </RichPanelDetails>
            </div>
        </RichPanel>
    );
}

interface CreateNewDestinationPanelProps {
    toggleCreateNewDestinationOpen: () => void;
    toggleCreateNewDestinationPinned: () => void;
    isCreateNewDestinationPinned: boolean;
    editingDestinationId?: string | null;
}

export function DestinationEditorPanel({
    toggleCreateNewDestinationOpen,
    isCreateNewDestinationPinned,
    toggleCreateNewDestinationPinned,
    editingDestinationId,
}: CreateNewDestinationPanelProps) {
    const { manageServerService } = useServices();
    const dispatch = useAppDispatch();
    const destinations = useAppSelector(remoteAttachmentsSelectors.destinations);

    const editingDestination = editingDestinationId
        ? destinations.find((d) => d.identifier === editingDestinationId)
        : null;

    const form = useForm<RemoteAttachmentsDestinationFormData>({
        resolver: remoteAttachmentsDestinationYupResolver,
        context: {
            destinations,
            currentIdentifier: editingDestinationId ?? undefined,
        },
        defaultValues: editingDestination
            ? {
                  provider: editingDestination.provider,
                  identifier: editingDestination.identifier,
                  disabled: editingDestination.disabled,
                  s3:
                      editingDestination.provider === "s3"
                          ? { ...defaultS3FormData, ...editingDestination.s3, isEnabled: true }
                          : null,
                  azure:
                      editingDestination.provider === "azure"
                          ? { ...defaultAzureFormData, ...editingDestination.azure, isEnabled: true }
                          : null,
              }
            : {
                  provider: "s3",
                  s3: { ...defaultS3FormData, isEnabled: true },
                  azure: defaultAzureFormData,
              },
    });

    const { control, watch, handleSubmit, trigger, setValue, formState } = form;
    const formValues = watch();

    console.log("maxym formState", formState.errors);

    const asyncTest = useAsyncCallback<Raven.Server.Web.System.NodeConnectionTestResult, []>(async () => {
        const isValid = await trigger(formValues.provider);
        if (!isValid) {
            return;
        }

        if (formValues.provider === "s3") {
            return manageServerService.testPeriodicBackupCredentials(
                "S3",
                mapS3ToDto({ ...formValues.s3, isEnabled: true })
            );
        } else {
            return manageServerService.testPeriodicBackupCredentials(
                "Azure",
                mapAzureToDto({ ...formValues.azure, isEnabled: true })
            );
        }
    });

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (name === "provider") {
                asyncTest.reset();

                if (values.provider === "s3") {
                    if (!values.s3) {
                        setValue("s3", { ...defaultS3FormData, isEnabled: true }, { shouldDirty: true });
                    } else {
                        setValue("s3.isEnabled", true);
                    }
                    if (!values.azure) {
                        setValue("azure", { ...defaultAzureFormData, isEnabled: false }, { shouldDirty: true });
                    } else {
                        setValue("azure.isEnabled", false);
                    }
                } else {
                    if (!values.azure) {
                        setValue("azure", { ...defaultAzureFormData, isEnabled: true }, { shouldDirty: true });
                    } else {
                        setValue("azure.isEnabled", true);
                    }
                    if (!values.s3) {
                        setValue("s3", { ...defaultS3FormData, isEnabled: false }, { shouldDirty: true });
                    } else {
                        setValue("s3.isEnabled", false);
                    }
                }
            }
        });
        return unsubscribe;
    }, []);

    const handleSaveDestination: SubmitHandler<RemoteAttachmentsDestinationFormData> = async (data) => {
        console.log("Saving destination:", data);

        const destinationData = {
            ...data,
            s3: data.provider === "s3" ? data.s3 : null,
            azure: data.provider === "azure" ? data.azure : null,
        } as RemoteAttachmentsDestinationFormData;

        if (editingDestinationId) {
            dispatch(
                remoteAttachmentsActions.updateDestination({
                    prevId: editingDestinationId,
                    destination: destinationData,
                })
            );
        } else {
            dispatch(remoteAttachmentsActions.addDestination(destinationData));
        }

        toggleCreateNewDestinationOpen();
    };

    return (
        <FormProvider {...form}>
            <div className="panel-bg-2 p-3 border-bottom border-secondary d-flex flex-wrap justify-content-between gap-2">
                <h3 className="m-0">
                    <Icon icon="global" addon="settings" color="primary" />
                    <span>{editingDestinationId ? "Edit Destination" : "Create new Destination"}</span>
                </h3>
                <div className="d-flex gap-2">
                    <Button variant="link" onClick={toggleCreateNewDestinationPinned} className="text-muted" size="sm">
                        <Icon size="sm" icon={isCreateNewDestinationPinned ? "pinned" : "pin"} margin="m-0" />
                    </Button>
                    <Button variant="link" className="text-muted" size="sm" onClick={toggleCreateNewDestinationOpen}>
                        <Icon size="sm" icon="close" margin="m-0" />
                    </Button>
                </div>
            </div>
            <div className="w-100 flex-grow-1 vstack p-4 overflow-auto">
                <FormGroup>
                    <FormLabel>Provider</FormLabel>
                    <FormSelect name="provider" control={control} options={providerOptions} />
                </FormGroup>

                {formValues.provider === "s3" && <RemoteAttachmentsS3Fields asyncTest={asyncTest} />}
                {formValues.provider === "azure" && <RemoteAttachmentsAzureFields asyncTest={asyncTest} />}
            </div>
            <div className="w-100 p-2 panel-bg-2 border-top d-flex justify-content-between border-secondary">
                <Button onClick={toggleCreateNewDestinationOpen} variant="link" className="text-muted">
                    Cancel
                </Button>
                <div className="d-flex gap-2">
                    <ButtonWithSpinner
                        type="button"
                        variant="info"
                        className="rounded-pill"
                        onClick={asyncTest.execute}
                        isSpinning={asyncTest.loading}
                        icon="rocket"
                    >
                        Test credentials
                    </ButtonWithSpinner>
                    <Button className="rounded-pill" onClick={handleSubmit(handleSaveDestination)}>
                        <Icon icon="save" />
                        Apply configuration
                    </Button>
                </div>
            </div>
        </FormProvider>
    );
}

const providerOptions = [
    {
        label: (
            <div>
                <Icon icon="aws" /> Amazon S3
            </div>
        ),
        value: "s3",
    },
    {
        label: (
            <div>
                <Icon icon="azure" /> Azure
            </div>
        ),
        value: "azure",
    },
];
