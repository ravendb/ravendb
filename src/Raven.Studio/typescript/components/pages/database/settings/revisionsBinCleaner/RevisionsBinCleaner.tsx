import React from "react";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "hooks/useServices";
import { Card, CardBody, Col, Collapse, Form, Row } from "reactstrap";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import {
    RevisionsBinCleanerFormData,
    revisionsBinCleanerYupResolver,
} from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import { useAsyncCallback } from "react-async-hook";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import { useEventsCollector } from "hooks/useEventsCollector";
import { tryHandleSubmit } from "components/utils/common";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput, FormSwitch } from "components/common/Form";
import { RevisionsBinCleanerInfoHub } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerInfoHub";
import { LoadingView } from "components/common/LoadingView";
import {
    mapToDto,
    mapToFormData,
} from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerUtils";
import { LoadError } from "components/common/LoadError";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import useRevisionsBinCleanerFormSideEffects from "components/pages/database/settings/revisionsBinCleaner/useRevisionsBinCleanerFormSideEffects";

export default function RevisionsBinCleaner() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const asyncGetRevisionsBinCleanerConfiguration = useAsyncCallback<RevisionsBinCleanerFormData>(async () =>
        mapToFormData(await databasesService.getRevisionsBinCleanerConfiguration(databaseName))
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

    const onSave: SubmitHandler<RevisionsBinCleanerFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("revisions-bin-configuration", "save");
            await databasesService.saveRevisionsBinCleanerConfiguration(databaseName, mapToDto(formData));

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
                        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
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
                                        <div className="vstack gap-2">
                                            <FormSwitch
                                                name="isRevisionsBinCleanerEnabled"
                                                disabled={!hasDatabaseAdminAccess}
                                                control={control}
                                            >
                                                Enable Revisions Bin Cleaner
                                            </FormSwitch>
                                            <div>
                                                <FormSwitch
                                                    name="isMinimumEntriesAgeToKeepEnabled"
                                                    control={control}
                                                    color="primary"
                                                    className={
                                                        formValues.isMinimumEntriesAgeToKeepEnabled &&
                                                        formValues.isRevisionsBinCleanerEnabled &&
                                                        "mb-3"
                                                    }
                                                    disabled={
                                                        !hasDatabaseAdminAccess ||
                                                        formState.isSubmitting ||
                                                        !formValues.isRevisionsBinCleanerEnabled
                                                    }
                                                >
                                                    Set minimum entries age to keep
                                                </FormSwitch>
                                                <Collapse
                                                    isOpen={
                                                        formValues.isMinimumEntriesAgeToKeepEnabled &&
                                                        formValues.isRevisionsBinCleanerEnabled
                                                    }
                                                >
                                                    <FormInput
                                                        name="minimumEntriesAgeToKeepInMin"
                                                        control={control}
                                                        type="number"
                                                        disabled={
                                                            !hasDatabaseAdminAccess ||
                                                            formState.isSubmitting ||
                                                            !formValues.isMinimumEntriesAgeToKeepEnabled
                                                        }
                                                        addon="minutes"
                                                    />
                                                </Collapse>
                                            </div>
                                            <div>
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
                                                    Set custom refresh frequency
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
                                            </div>
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>
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
