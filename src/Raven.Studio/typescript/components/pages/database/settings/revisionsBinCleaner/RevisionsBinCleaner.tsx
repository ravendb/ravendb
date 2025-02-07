import React from "react";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "hooks/useServices";
import { Card, CardBody, Col, Collapse, Form, FormGroup, Row, UncontrolledPopover } from "reactstrap";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import {
    RevisionsBinCleanerFormData,
    revisionsBinCleanerYupResolver,
} from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import { useAsyncCallback } from "react-async-hook";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import { useEventsCollector } from "hooks/useEventsCollector";
import { tryHandleSubmit } from "components/utils/common";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormDurationPicker, FormInput, FormSwitch } from "components/common/Form";
import { RevisionsBinCleanerInfoHub } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerInfoHub";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import useRevisionsBinCleanerFormSideEffects from "components/pages/database/settings/revisionsBinCleaner/useRevisionsBinCleanerFormSideEffects";
import { revisionsBinCleanerUtils } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerUtils";
import { Icon } from "components/common/Icon";

export default function RevisionsBinCleaner() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const asyncGetRevisionsBinCleanerConfiguration = useAsyncCallback<RevisionsBinCleanerFormData>(async () =>
        revisionsBinCleanerUtils.mapToFormData(await databasesService.getRevisionsBinCleanerConfiguration(databaseName))
    );

    const { handleSubmit, control, formState, reset, setValue, watch } = useForm<Partial<RevisionsBinCleanerFormData>>({
        resolver: revisionsBinCleanerYupResolver,
        mode: "all",
        defaultValues: asyncGetRevisionsBinCleanerConfiguration.execute,
    });

    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control });

    const { reportEvent } = useEventsCollector();

    useRevisionsBinCleanerFormSideEffects(watch, setValue);

    const handleSave: SubmitHandler<RevisionsBinCleanerFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("revisions-bin-configuration", "save");
            await databasesService.saveRevisionsBinCleanerConfiguration(
                databaseName,
                revisionsBinCleanerUtils.mapToDto(formData)
            );

            reset(formData);
        });
    };

    if (
        asyncGetRevisionsBinCleanerConfiguration.status === "not-requested" ||
        asyncGetRevisionsBinCleanerConfiguration.status === "loading"
    ) {
        return <LoadingView />;
    }

    if (asyncGetRevisionsBinCleanerConfiguration.status === "error") {
        return (
            <LoadError
                error="Unable to load revisions bin cleaner"
                refresh={asyncGetRevisionsBinCleanerConfiguration.execute}
            />
        );
    }

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <Form onSubmit={handleSubmit(handleSave)} autoComplete="off">
                            <AboutViewHeading title="Revisions Bin Cleaner" icon="revisions-bin" />
                            {hasDatabaseAdminAccess && (
                                <ButtonWithSpinner
                                    type="submit"
                                    color="primary"
                                    className="mb-3"
                                    icon="save"
                                    disabled={!formState.isDirty}
                                    isSpinning={formState.isSubmitting}
                                >
                                    Save
                                </ButtonWithSpinner>
                            )}
                            <Col>
                                <Card>
                                    <CardBody>
                                        <FormGroup>
                                            <FormSwitch
                                                name="isRevisionsBinCleanerEnabled"
                                                disabled={!hasDatabaseAdminAccess}
                                                control={control}
                                            >
                                                Enable Revisions Bin Cleaner
                                            </FormSwitch>
                                        </FormGroup>
                                        <FormGroup className="d-flex gap-2 align-items-center">
                                            <FormSwitch
                                                name="isMinimumEntriesAgeToKeepEnabled"
                                                control={control}
                                                color="primary"
                                                disabled={
                                                    !hasDatabaseAdminAccess ||
                                                    formState.isSubmitting ||
                                                    !formValues.isRevisionsBinCleanerEnabled
                                                }
                                            >
                                                Set minimum entries age to keep
                                            </FormSwitch>
                                            {formValues.isRevisionsBinCleanerEnabled &&
                                                !formValues.isMinimumEntriesAgeToKeepEnabled && (
                                                    <>
                                                        <Icon
                                                            id="setMinimumEntriesAgeToKeep"
                                                            icon="warning"
                                                            color="warning"
                                                        />
                                                        <UncontrolledPopover
                                                            target="setMinimumEntriesAgeToKeep"
                                                            trigger="hover"
                                                            container="PopoverContainer"
                                                            placement="right"
                                                        >
                                                            <div className="p-3">
                                                                All items in the Revisions Bin will be deleted when
                                                                &#39;Set minimum entries age to keep&#39; is toggled
                                                                off.
                                                            </div>
                                                        </UncontrolledPopover>
                                                    </>
                                                )}
                                        </FormGroup>
                                        <Collapse
                                            data-testid="collapse"
                                            isOpen={
                                                formValues.isMinimumEntriesAgeToKeepEnabled &&
                                                formValues.isRevisionsBinCleanerEnabled
                                            }
                                        >
                                            <FormGroup data-testid="durationPicker">
                                                <FormDurationPicker
                                                    control={control}
                                                    disabled={
                                                        !hasDatabaseAdminAccess ||
                                                        formState.isSubmitting ||
                                                        !formValues.isMinimumEntriesAgeToKeepEnabled
                                                    }
                                                    placeholder={{
                                                        days: "Default (30)",
                                                        hours: "Default (0)",
                                                        minutes: "Default (0)",
                                                    }}
                                                    name="minimumEntriesAgeToKeep"
                                                    showDays
                                                />
                                            </FormGroup>
                                        </Collapse>
                                        <FormGroup>
                                            <FormSwitch
                                                name="isRefreshFrequencyEnabled"
                                                control={control}
                                                color="primary"
                                                className="mb-3"
                                                disabled={
                                                    !hasDatabaseAdminAccess ||
                                                    formState.isSubmitting ||
                                                    !formValues.isRevisionsBinCleanerEnabled
                                                }
                                            >
                                                Set custom cleaner frequency
                                            </FormSwitch>
                                            <FormInput
                                                name="refreshFrequencyInSec"
                                                control={control}
                                                type="number"
                                                disabled={
                                                    !hasDatabaseAdminAccess ||
                                                    formState.isSubmitting ||
                                                    !formValues.isRefreshFrequencyEnabled
                                                }
                                                placeholder="Default (300)"
                                                addon="seconds"
                                            />
                                        </FormGroup>
                                    </CardBody>
                                </Card>
                            </Col>
                            <div id="PopoverContainer"></div>
                        </Form>
                    </Col>
                    <Col sm={12} lg={4}>
                        <RevisionsBinCleanerInfoHub />
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
