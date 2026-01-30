import {
    RemoteAttachmentsDestinationFormData,
    remoteAttachmentsDestinationYupResolver,
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
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
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
import { useViewSheet, ViewSheet } from "components/common/splitView/ViewSheet";
import { remoteAttachmentsConstants } from "components/pages/database/settings/remoteAttachments/remoteAttachmentsConstants";
import { remoteAttachmentsUtils } from "components/pages/database/settings/remoteAttachments/remoteAttachmentsUtils";
import { OptionWithIcon, SelectOptionWithIcon, SingleValueWithIcon } from "components/common/select/Select";
import { storageClassOptions } from "components/utils/common";
import { ConditionalPopover } from "components/common/ConditionalPopover";

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
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const isModified = useAppSelector(remoteAttachmentsSelectors.isDestinationModified(identifier));

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
                    <RichPanelActions>
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasDatabaseAdminAccess,
                                message:
                                    "You don't have the required permissions to modify destinations (Database Admin access required)",
                            }}
                        >
                            <Button
                                variant={disabled ? "success" : "secondary"}
                                onClick={() => onToggle(identifier)}
                                disabled={!hasDatabaseAdminAccess}
                            >
                                <Icon icon={disabled ? "play" : "disable"} />
                                {disabled ? "Enable" : "Disable"}
                            </Button>
                        </ConditionalPopover>
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasDatabaseAdminAccess,
                                message:
                                    "You don't have the required permissions to edit destinations (Database Admin access required)",
                            }}
                        >
                            <Button
                                variant="secondary"
                                onClick={() => onEdit(identifier)}
                                disabled={!hasDatabaseAdminAccess}
                            >
                                <Icon icon="edit" margin="m-0" />
                            </Button>
                        </ConditionalPopover>
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasDatabaseAdminAccess,
                                message:
                                    "You don't have the required permissions to delete destinations (Database Admin access required)",
                            }}
                        >
                            <Button
                                variant="danger"
                                onClick={() => onDelete(identifier)}
                                disabled={!hasDatabaseAdminAccess}
                            >
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        </ConditionalPopover>
                    </RichPanelActions>
                </RichPanelHeader>
                <RichPanelDetails>
                    <RichPanelDetailItem size="sm" label="Destination">
                        <Icon icon={remoteAttachmentsUtils.getProviderIcon(provider)} />
                        {remoteAttachmentsUtils.getProviderName(provider)}
                    </RichPanelDetailItem>
                </RichPanelDetails>
            </div>
        </RichPanel>
    );
}

interface CreateNewDestinationPanelProps {
    editingDestinationId?: string;
}

export function DestinationEditorPanel({ editingDestinationId }: CreateNewDestinationPanelProps) {
    const { manageServerService } = useServices();
    const dispatch = useAppDispatch();
    const destinations = useAppSelector(remoteAttachmentsSelectors.destinations);

    const editingDestination = editingDestinationId
        ? destinations.find((d) => d.identifier === editingDestinationId)
        : null;

    const { close } = useViewSheet();

    const form = useForm<RemoteAttachmentsDestinationFormData>({
        resolver: remoteAttachmentsDestinationYupResolver,
        context: {
            destinations,
            currentIdentifier: editingDestinationId ?? undefined,
        },
        defaultValues: getDefaultValues(editingDestination),
    });

    const { control, watch, handleSubmit, trigger, setValue } = form;
    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback<Raven.Server.Web.System.NodeConnectionTestResult, []>(async () => {
        const isValid = await trigger(formValues.provider);
        if (!isValid) {
            return;
        }

        switch (formValues.provider) {
            case "s3":
                return manageServerService.testPeriodicBackupCredentials(
                    "S3",
                    mapS3ToDto({ ...formValues.s3, isEnabled: true })
                );
            case "azure":
                return manageServerService.testPeriodicBackupCredentials(
                    "Azure",
                    mapAzureToDto({ ...formValues.azure, isEnabled: true })
                );
            default:
                break;
        }
    });

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (name === "provider") {
                asyncTest.reset();

                const setProviderState = (provider: DestinationProviderType, isEnabled: boolean) => {
                    const defaults = provider === "s3" ? defaultS3FormData : defaultAzureFormData;
                    if (!values[provider]) {
                        setValue(provider, { ...defaults, isEnabled }, { shouldDirty: true });
                    } else {
                        setValue(`${provider}.isEnabled`, isEnabled);
                    }
                };

                const activeProvider = values.provider;
                const providers = remoteAttachmentsConstants.destinationProviderList;

                providers.forEach((p) => setProviderState(p, p === activeProvider));
            }
        });
        return unsubscribe;
    }, []);

    const handleSaveDestination: SubmitHandler<RemoteAttachmentsDestinationFormData> = async (data) => {
        const destinationData = {
            ...data,
            s3: data.provider === "s3" ? data.s3 : null,
            azure: data.provider === "azure" ? data.azure : null,
        };

        if (editingDestinationId) {
            dispatch(
                remoteAttachmentsActions.destinationUpdated({
                    prevId: editingDestinationId,
                    destination: destinationData,
                })
            );
        } else {
            dispatch(remoteAttachmentsActions.destinationAdded(destinationData));
        }

        close();
    };

    return (
        <FormProvider {...form}>
            <form className="h-100 d-flex flex-column" onSubmit={handleSubmit(handleSaveDestination)}>
                <ViewSheet.Header className="panel-bg-2 align-items-center" isPinHidden>
                    <h3 className="m-0">
                        <Icon icon="global" addon="settings" color="primary" margin="me-2" />
                        <span>{editingDestinationId ? "Edit Destination" : "Define New Destination"}</span>
                    </h3>
                </ViewSheet.Header>
                <ViewSheet.Body className="w-100 flex-grow-1 vstack p-4 overflow-auto">
                    <FormGroup marginClass="mb-0">
                        <FormLabel>Provider</FormLabel>
                        <FormSelect
                            name="provider"
                            components={{
                                Option: OptionWithIcon,
                                SingleValue: SingleValueWithIcon,
                            }}
                            control={control}
                            options={providerOptions}
                        />
                    </FormGroup>
                    {formValues.provider === "s3" && <RemoteAttachmentsS3Fields asyncTest={asyncTest} />}
                    {formValues.provider === "azure" && <RemoteAttachmentsAzureFields asyncTest={asyncTest} />}
                </ViewSheet.Body>
                <ViewSheet.Footer className="w-100 p-2 panel-bg-2 border-top d-flex justify-content-between border-secondary">
                    <Button onClick={close} variant="link" className="text-muted">
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
                        <Button className="rounded-pill" type="submit">
                            <Icon icon="save" />
                            Apply configuration
                        </Button>
                    </div>
                </ViewSheet.Footer>
            </form>
        </FormProvider>
    );
}

function getDefaultValues(
    editingDestination?: RemoteAttachmentsDestinationFormData
): RemoteAttachmentsDestinationFormData {
    if (editingDestination) {
        return {
            provider: editingDestination.provider,
            identifier: editingDestination.identifier,
            disabled: editingDestination.disabled,
            s3:
                editingDestination.provider === "s3"
                    ? {
                          ...defaultS3FormData,
                          ...editingDestination.s3,
                          isEnabled: true,
                      }
                    : null,
            azure:
                editingDestination.provider === "azure"
                    ? { ...defaultAzureFormData, ...editingDestination.azure, isEnabled: true }
                    : null,
        };
    }

    return {
        provider: "s3",
        s3: { ...defaultS3FormData, storageClass: storageClassOptions[0].value, isEnabled: true },
        azure: defaultAzureFormData,
    };
}
type DestinationProviderType = RemoteAttachmentsDestinationFormData["provider"];

const providerOptions: SelectOptionWithIcon[] = [
    {
        label: "Amazon S3",
        value: "s3",
        icon: "aws",
    },
    {
        label: "Azure",
        value: "azure",
        icon: "azure",
    },
];
