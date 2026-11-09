import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { FormProvider, SubmitHandler, useForm, useFormContext, useWatch } from "react-hook-form";
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
import { FormSwitch, DIRTY_AND_VALIDATE } from "components/common/Form";
import { FormInputWithDefault } from "components/common/FieldWithDefault";
import FrequencyField from "components/common/FrequencyField";
import { Icon } from "components/common/Icon";
import React from "react";
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
import { ConditionalPopover } from "components/common/ConditionalPopover";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import { getAccessRequiredMessage } from "components/utils/accessUtils";
import classNames from "classnames";

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
        defaultValues: async () => {
            const dto = await dispatch(remoteAttachmentsActions.fetchRemoteAttachments(databaseName))
                .unwrap()
                .catch((): null => null);

            return remoteAttachmentsUtils.mapFromDto(dto);
        },
    });

    const { handleSubmit, formState, reset } = form;

    const hasRemoteAttachments = useAppSelector(licenseSelectors.statusValue("HasRemoteAttachments"));

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

    if (loadStatus === "loading" || formState.isLoading) {
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
                                        message: getAccessRequiredMessage("DatabaseAdmin"),
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
                            </div>
                            <DestinationsList />
                        </Col>
                        <Col sm={12} lg={4}>
                            <RemoteAttachmentsInfoHub />
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
    const hasRemoteAttachments = useAppSelector(licenseSelectors.statusValue("HasRemoteAttachments"));

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
                            message: getAccessRequiredMessage("DatabaseAdmin"),
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
            <div className={classNames(!hasRemoteAttachments && "item-disabled pe-none")}>
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
        </div>
    );
}

function RemoteAttachmentsSettingsCard() {
    const { control, formState, setValue } = useFormContext<RemoteAttachmentsFormData>();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const formValues = useWatch({ control });
    const isSettingsDisabled = !hasDatabaseAdminAccess || !formValues.isRemoteAttachmentsEnabled;

    const resetSettingsOnToggle = (isEnabled: boolean) => {
        if (isEnabled) {
            return;
        }

        setValue("checkFrequencyInSec", null, DIRTY_AND_VALIDATE);
        setValue("maxItemsToProcess", null, DIRTY_AND_VALIDATE);
        setValue("concurrentUploads", null, DIRTY_AND_VALIDATE);
    };

    return (
        <Card>
            <Card.Body className="vstack gap-3">
                <FormSwitch
                    name="isRemoteAttachmentsEnabled"
                    control={control}
                    color="primary"
                    disabled={!hasDatabaseAdminAccess || formState.isSubmitting}
                    afterChange={resetSettingsOnToggle}
                >
                    Enable Remote Attachments
                </FormSwitch>
                <FrequencyField
                    control={control}
                    name="checkFrequencyInSec"
                    label="Interval between remote attachments task runs"
                    disabled={isSettingsDisabled}
                />
                <FormInputWithDefault
                    control={control}
                    name="maxItemsToProcess"
                    label="Max number of attachments to process in a single run"
                    disabled={isSettingsDisabled}
                    type="number"
                    defaultValueLabel="unlimited"
                    addon="attachments"
                />
                <FormInputWithDefault
                    control={control}
                    name="concurrentUploads"
                    label="Max number of concurrent uploads"
                    disabled={isSettingsDisabled}
                    type="number"
                    defaultValueLabel="8"
                    addon="attachments"
                />
            </Card.Body>
        </Card>
    );
}
