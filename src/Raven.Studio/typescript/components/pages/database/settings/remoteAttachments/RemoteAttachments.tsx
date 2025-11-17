import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { FormProvider, SubmitHandler, useForm, useFormContext, UseFormSetValue, UseFormWatch } from "react-hook-form";
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
import classNames from "classnames";
import { useViewSheet, ViewSheet } from "components/common/splitView/ViewSheet";

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

    const { handleSubmit, watch, formState, setValue, reset } = form;

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

    useRemoteAttachmentsSideEffects({ setValue, watch });

    if (loadStatus === "loading") {
        return <LoadingView />;
    }

    if (loadStatus === "failure") {
        return (
            <LoadError
                error="Unable to load remote attachments"
                refresh={() => dispatch(remoteAttachmentsActions.fetchRemoteAttachments(databaseName))}
            />
        );
    }

    return (
        <div className="content-padding position-relative h-100">
            <FormProvider {...form}>
                <Form onSubmit={handleSubmit(handleSave)}>
                    <Col xxl={12}>
                        <Row className="gy-sm">
                            <Col>
                                <AboutViewHeading title="Remote attachments" icon="remote-attachment" />
                                {hasDatabaseAdminAccess && (
                                    <ButtonWithSpinner
                                        type="submit"
                                        variant="primary"
                                        className="mb-3"
                                        disabled={!isAnyModified && !formState.isDirty}
                                        icon="save"
                                        isSpinning={formState.isSubmitting}
                                    >
                                        Save
                                    </ButtonWithSpinner>
                                )}
                                <div className="my-4">
                                    <HrHeader>
                                        <Icon icon="config" />
                                        Configuration
                                    </HrHeader>
                                    <RemoteAttachmentsSettingsCard />
                                </div>
                                <DestinationsList />
                            </Col>
                            <Col sm={12} lg={4}>
                                <RemoteAttachmentsInfoHub />
                            </Col>
                        </Row>
                    </Col>
                </Form>
            </FormProvider>
        </div>
    );
}

function DestinationsList() {
    const confirm = useConfirm();
    const { getValues } = useFormContext<RemoteAttachmentsFormData>();
    const dispatch = useAppDispatch();
    const destinations = useAppSelector(remoteAttachmentsSelectors.destinations);
    const destinationsTotal = useAppSelector(remoteAttachmentsSelectors.destinationsTotal);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const formValues = getValues();

    const deleteDestination = async (id: string) => {
        const confirmed = await confirm({
            icon: "remote-attachment",
            actionColor: "danger",
            title: "You're about to delete remote attachment",
            message: "This action cannot be undone.",
            confirmText: "Delete",
        });

        if (confirmed) {
            dispatch(remoteAttachmentsActions.removeDestination(id));
        }
    };

    const editDestination = async (id: string) => {
        handleOpenSheet(id);
    };

    const toggleDestination = async (id: string) => {
        dispatch(remoteAttachmentsActions.toggleDestinationDisabled(id));
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
                    hasDatabaseAdminAccess && (
                        <Button
                            disabled={!formValues.isRemoteAttachmentsEnabled}
                            size="sm"
                            onClick={() => handleOpenSheet()}
                            variant="info"
                            className="rounded-pill"
                        >
                            <Icon icon="plus" size="sm" />
                            Add new
                        </Button>
                    )
                }
                count={destinationsTotal}
            >
                <Icon icon="global" />
                Destinations
            </HrHeader>
            <div
                className={classNames({
                    "opacity-25": !formValues.isRemoteAttachmentsEnabled,
                })}
            >
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
    const { control, formState, watch } = useFormContext<RemoteAttachmentsFormData>();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const formValues = watch();

    return (
        <Card>
            <Card.Body>
                <FormSwitch
                    name="isRemoteAttachmentsEnabled"
                    control={control}
                    className="mb-3"
                    color="primary"
                    disabled={!hasDatabaseAdminAccess || formState.isSubmitting}
                >
                    Enable Remote Attachments
                </FormSwitch>
                <FormGroup data-testid="retireFrequency">
                    <FormSwitch
                        name="isCheckFrequencyInSecEnabled"
                        control={control}
                        className="mb-3"
                        color="primary"
                        disabled={
                            !hasDatabaseAdminAccess || formState.isSubmitting || !formValues.isRemoteAttachmentsEnabled
                        }
                    >
                        Set custom retire frequency
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
                        className="mb-3"
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
                        className="mb-3"
                        disabled={
                            !hasDatabaseAdminAccess || formState.isSubmitting || !formValues.isRemoteAttachmentsEnabled
                        }
                    >
                        Allow concurrent uploads
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

interface UseDestinationsSideEffectsProps {
    setValue: UseFormSetValue<RemoteAttachmentsFormData>;
    watch: UseFormWatch<RemoteAttachmentsFormData>;
}

const useRemoteAttachmentsSideEffects = ({ watch, setValue }: UseDestinationsSideEffectsProps) => {
    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (name === "isRemoteAttachmentsEnabled" && !values.isRemoteAttachmentsEnabled) {
                setValue("isCheckFrequencyInSecEnabled", false);
                setValue("isMaxItemsToProcessEnabled", false);
                setValue("isConcurrentUploadsEnabled", false);
            }
        });
        return unsubscribe;
    }, [setValue, watch]);
};
