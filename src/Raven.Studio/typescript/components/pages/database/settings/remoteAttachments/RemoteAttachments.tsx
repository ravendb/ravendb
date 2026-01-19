import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { FormProvider, SubmitHandler, useForm, useFormContext, UseFormReturn, useWatch } from "react-hook-form";
import { useEventsCollector } from "hooks/useEventsCollector";
import { tryHandleSubmit } from "components/utils/common";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import Form from "react-bootstrap/Form";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import Card from "react-bootstrap/Card";
import { FormGroup, FormInput, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import React, { useEffect } from "react";
import {
    RemoteAttachmentsFormData,
    remoteAttachmentsYupResolver,
} from "components/pages/database/settings/remoteAttachments/remoteAttachmentsValidation";
import { HrHeader } from "components/common/HrHeader";
import Button from "react-bootstrap/Button";
import { remoteAttachmentsActions } from "./store/remoteAttachmentsSlice";
import { remoteAttachmentsSelectors } from "./store/remoteAttachmentsSliceSelectors";
import { remoteAttachmentsUtils } from "components/pages/database/settings/remoteAttachments/remoteAttachmentsUtils";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import {
    DestinationEditorPanel,
    DestinationPanel,
} from "components/pages/database/settings/remoteAttachments/partials/DestinationPanel";
import useConfirm from "components/common/ConfirmDialog";
import { RemoteAttachmentsInfoHub } from "components/pages/database/settings/remoteAttachments/partials/RemoteAttachmentsInfoHub";
import { useViewSheet, ViewSheet } from "components/common/splitView/ViewSheet";
import { EmptySet } from "components/common/EmptySet";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";

export default function RemoteAttachments() {
    const { reportEvent } = useEventsCollector();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const dispatch = useAppDispatch();
    const loadStatus = useAppSelector(remoteAttachmentsSelectors.loadStatus);

    const destinations = useAppSelector(remoteAttachmentsSelectors.destinations);
    const isAnyModified = useAppSelector(remoteAttachmentsSelectors.isAnyModified);

    const form = useForm<RemoteAttachmentsFormData>({
        resolver: remoteAttachmentsYupResolver,
        mode: "all",
        defaultValues: async () =>
            remoteAttachmentsUtils.mapFromDto(
                await dispatch(remoteAttachmentsActions.fetchRemoteAttachments(databaseName)).unwrap()
            ),
    });

    const { handleSubmit, formState, reset } = form;

    const hasRemoteAttachments = useAppSelector(licenseSelectors.statusValue("HasRemoteAttachments"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasRemoteAttachments,
            },
        ],
    });

    useDirtyFlag(formState.isDirty || isAnyModified);

    const handleSave: SubmitHandler<RemoteAttachmentsFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("remote-attachments-configuration", "save");
            await dispatch(
                remoteAttachmentsActions.saveRemoteAttachments({
                    databaseName,
                    data: { ...formData, destinations },
                })
            ).unwrap();

            reset(formData);
        });
    };

    useRemoteAttachmentsSideEffects(form);

    if (loadStatus === "loading") {
        return <LoadingView />;
    }

    if (loadStatus === "failure") {
        return (
            <LoadError
                error="Unable to load remote attachments"
                refresh={async () => {
                    const dto = await dispatch(remoteAttachmentsActions.fetchRemoteAttachments(databaseName)).unwrap();
                    form.reset(remoteAttachmentsUtils.mapFromDto(dto));
                }}
            />
        );
    }

    return (
        <div className="content-padding position-relative h-100">
            <FormProvider {...form}>
                <Form onSubmit={handleSubmit(handleSave)}>
                    <Row className="gy-sm">
                        <Col>
                            <AboutViewHeading
                                title="Remote Attachments"
                                icon="remote-attachment"
                                licenseBadgeText={hasRemoteAttachments ? null : "Enterprise"}
                            />
                            <ConditionalPopover
                                conditions={[
                                    {
                                        isActive: !hasRemoteAttachments,
                                        message: <FeatureNotAvailableInYourLicensePopoverBody />,
                                    },
                                    {
                                        isActive: !hasDatabaseAdminAccess,
                                        message:
                                            "You don't have the required permissions to save changes (Database Admin access required)",
                                    },
                                ]}
                            >
                                <ButtonWithSpinner
                                    type="submit"
                                    variant="primary"
                                    className="mb-3"
                                    disabled={
                                        !hasDatabaseAdminAccess ||
                                        !hasRemoteAttachments ||
                                        (!isAnyModified && !formState.isDirty)
                                    }
                                    icon="save"
                                    isSpinning={formState.isSubmitting}
                                >
                                    Save
                                </ButtonWithSpinner>
                            </ConditionalPopover>
                            <div className={hasRemoteAttachments ? "" : "item-disabled pe-none"}>
                                <RemoteAttachmentsSettingsCard />
                                <DestinationsList />
                            </div>
                        </Col>
                        <Col sm={12} lg={4}>
                            <RemoteAttachmentsInfoHub
                                hasRemoteAttachments={hasRemoteAttachments}
                                featureAvailability={featureAvailability}
                            />
                        </Col>
                    </Row>
                </Form>
            </FormProvider>
        </div>
    );
}

function DestinationsList() {
    const confirm = useConfirm();
    const dispatch = useAppDispatch();
    const destinations = useAppSelector(remoteAttachmentsSelectors.destinations);
    const destinationsTotal = useAppSelector(remoteAttachmentsSelectors.destinationsTotal);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const deleteDestination = async (id: string) => {
        const confirmed = await confirm({
            icon: "remote-attachment",
            actionColor: "danger",
            title: `You are about to delete the remote destination: '${id}'`,
            message: "This action cannot be undone.",
            confirmText: "Delete",
        });

        if (confirmed) {
            dispatch(remoteAttachmentsActions.destinationRemoved(id));
        }
    };

    const editDestination = async (id: string) => {
        handleOpenSheet(id);
    };

    const toggleDestination = async (id: string) => {
        dispatch(remoteAttachmentsActions.toggledDestinationDisabled(id));
    };

    const { open } = useViewSheet();

    const handleOpenSheet = (editDestinationId?: string) => {
        open({
            component: (
                <ViewSheet className="h-100">
                    <DestinationEditorPanel editingDestinationId={editDestinationId} />
                </ViewSheet>
            ),
        });
    };

    return (
        <div className="mb-4">
            <HrHeader
                right={
                    <ConditionalPopover
                        conditions={{
                            isActive: !hasDatabaseAdminAccess,
                            message:
                                "You don't have the required permissions to add destinations (Database Admin access required)",
                        }}
                    >
                        <Button
                            size="sm"
                            onClick={() => handleOpenSheet()}
                            variant="info"
                            className="rounded-pill"
                            title="Click to define a new destination"
                            disabled={!hasDatabaseAdminAccess}
                        >
                            <Icon icon="plus" size="sm" />
                            Add new
                        </Button>
                    </ConditionalPopover>
                }
                count={destinationsTotal}
            >
                <Icon icon="global" />
                Destinations
            </HrHeader>
            {destinations.length === 0 && <EmptySet>No destinations have been defined.</EmptySet>}
            {destinations.map((field, index) => (
                <DestinationPanel
                    {...field}
                    key={index}
                    onEdit={editDestination}
                    onDelete={deleteDestination}
                    onToggle={toggleDestination}
                />
            ))}
        </div>
    );
}

function RemoteAttachmentsSettingsCard() {
    const { control, formState } = useFormContext<RemoteAttachmentsFormData>();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const formValues = useWatch({ control });
    return (
        <Card>
            <Card.Body>
                <FormSwitch
                    name="isRemoteAttachmentsEnabled"
                    control={control}
                    color="primary"
                    className="mb-2"
                    disabled={!hasDatabaseAdminAccess || formState.isSubmitting}
                >
                    Enable Remote Attachments
                </FormSwitch>
                <FormGroup data-testid="retireFrequency">
                    <FormSwitch
                        name="isCheckFrequencyInSecEnabled"
                        control={control}
                        color="primary"
                        disabled={
                            !hasDatabaseAdminAccess || formState.isSubmitting || !formValues.isRemoteAttachmentsEnabled
                        }
                    >
                        Set interval between remote attachments task runs
                    </FormSwitch>
                    <FormInput
                        name="checkFrequencyInSec"
                        type="number"
                        control={control}
                        addon="seconds"
                        placeholder="Default (60)"
                        disabled={
                            !hasDatabaseAdminAccess ||
                            formState.isSubmitting ||
                            !formValues.isCheckFrequencyInSecEnabled ||
                            !formValues.isRemoteAttachmentsEnabled
                        }
                    />
                </FormGroup>
                <FormGroup>
                    <FormSwitch
                        name="isMaxItemsToProcessEnabled"
                        control={control}
                        color="primary"
                        disabled={
                            !hasDatabaseAdminAccess || formState.isSubmitting || !formValues.isRemoteAttachmentsEnabled
                        }
                    >
                        Set max number of attachments to process in a single run
                    </FormSwitch>
                    <FormInput
                        name="maxItemsToProcess"
                        control={control}
                        type="number"
                        placeholder="Default (unlimited)"
                        disabled={
                            !hasDatabaseAdminAccess ||
                            formState.isSubmitting ||
                            !formValues.isRemoteAttachmentsEnabled ||
                            !formValues.isMaxItemsToProcessEnabled
                        }
                        addon="attachments"
                    />
                </FormGroup>
                <FormGroup marginClass="mb-0">
                    <FormSwitch
                        name="isConcurrentUploadsEnabled"
                        control={control}
                        color="primary"
                        disabled={
                            !hasDatabaseAdminAccess || formState.isSubmitting || !formValues.isRemoteAttachmentsEnabled
                        }
                    >
                        Set max number of concurrent uploads
                    </FormSwitch>
                    <FormInput
                        name="concurrentUploads"
                        control={control}
                        type="number"
                        disabled={
                            !hasDatabaseAdminAccess ||
                            formState.isSubmitting ||
                            !formValues.isRemoteAttachmentsEnabled ||
                            !formValues.isConcurrentUploadsEnabled
                        }
                        placeholder="Default (8)"
                        addon="attachments"
                    />
                </FormGroup>
            </Card.Body>
        </Card>
    );
}

const useRemoteAttachmentsSideEffects = ({ watch, setValue }: UseFormReturn<RemoteAttachmentsFormData>) => {
    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (name === "isRemoteAttachmentsEnabled" && !values.isRemoteAttachmentsEnabled) {
                setValue("isCheckFrequencyInSecEnabled", false, { shouldValidate: true });
                setValue("isMaxItemsToProcessEnabled", false, { shouldValidate: true });
                setValue("isConcurrentUploadsEnabled", false, { shouldValidate: true });
            }
        });
        return unsubscribe;
    }, [setValue, watch]);
};

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Remote Attachments",
        featureIcon: "remote-attachment",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
