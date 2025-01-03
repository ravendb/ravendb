import React, { useState } from "react";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "hooks/useServices";
import { Card, CardBody, Col, Form, Row } from "reactstrap";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import {
    RevisionsBinCleanerFormData,
    revisionsBinCleanerYupResolver,
} from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import { useAsyncCallback } from "react-async-hook";
import RevisionsBinConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;
import { useDirtyFlag } from "hooks/useDirtyFlag";
import { useEventsCollector } from "hooks/useEventsCollector";
import { tryHandleSubmit } from "components/utils/common";
import messagePublisher from "common/messagePublisher";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput, FormSwitch } from "components/common/Form";
import { RevisionsBinCleanerInfoHub } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerInfoHub";

export default function RevisionsBinCleaner() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();

    const asyncGetRevisionsBinCleanerConfiguration = useAsyncCallback<RevisionsBinCleanerFormData>(async () =>
        // mapToFormData(await databasesService.getRevisionsBinCleanerConfiguration(databaseName))
        mapToFormData(null)
    );

    const { handleSubmit, control, formState, reset, setValue, watch } = useForm<RevisionsBinCleanerFormData>({
        resolver: revisionsBinCleanerYupResolver,
        mode: "all",
        defaultValues: asyncGetRevisionsBinCleanerConfiguration.execute,
    });

    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control: control });

    const { reportEvent } = useEventsCollector();

    // useEffect(() => {
    //     const { unsubscribe } = watch((values, { name }) => {
    //         switch (name) {
    //             case "isRevisionsBinCleanerEnabled": {
    //                 if (values.isRevisionsBinCleanerEnabled) {
    //                     setValue("isRefreshFrequencyEnabled", true, { shouldValidate: true});
    //                 } else {
    //                     setValue("isRefreshFrequencyEnabled", false, { shouldValidate: true});
    //                     setValue("isMinimumEntriesAgeToKeepEnabled", false, { shouldValidate: true});
    //                 }
    //                 break;
    //             }
    //             case "isMinimumEntriesAgeToKeepEnabled": {
    //                 if (values.isMinimumEntriesAgeToKeepEnabled) {
    //                     setValue("minimumEntriesAgeToKeepInMin",)
    //                 }
    //             }
    //             }
    //         }
    //     }
    // })

    const onSave: SubmitHandler<RevisionsBinCleanerFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("revisions-bin-configuration", "save");

            await databasesService.saveRevisionsBinCleanerConfiguration(databaseName, {
                Disabled: !formData.isRevisionsBinCleanerEnabled,
                MinimumEntriesAgeToKeepInMin: formData.isMinimumEntriesAgeToKeepEnabled
                    ? formData.minimumEntriesAgeToKeepInMin
                    : null,
                RefreshFrequencyInSec: formData.isRefreshFrequencyEnabled ? formData.refreshFrequencyInSec : null,
            });

            messagePublisher.reportSuccess("Revisions bin cleaner configuration saved successfully");
            activeDatabaseTracker.default.database().hasRevisionsConfiguration(formData.isRevisionsBinCleanerEnabled);

            reset(formData);
        });
    };

    const [isMinimumEntriesAgeToKeepOpen, setIsMinimumEntriesToKeepOpen] = useState(false);
    const toggleMinimumEntriesAgeToKeep = () => setIsMinimumEntriesToKeepOpen(!isMinimumEntriesAgeToKeepOpen);

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
                            <Col>
                                <Card>
                                    <CardBody>
                                        <div className="vstack gap-2">
                                            <FormSwitch name="isRevisionsBinCleanerEnabled" control={control}>
                                                Enable Revisions Bin Cleaner
                                            </FormSwitch>
                                            <div>
                                                <FormSwitch
                                                    id="toggleMinEntriesAgeToKeep"
                                                    name="isMinimumEntriesAgeToKeepEnabled"
                                                    control={control}
                                                    className={formValues.isMinimumEntriesAgeToKeepEnabled && "mb-3"}
                                                    disabled={
                                                        formState.isSubmitting ||
                                                        !formValues.isRevisionsBinCleanerEnabled
                                                    }
                                                >
                                                    Set minimum entries age to keep
                                                </FormSwitch>
                                                {formValues.isMinimumEntriesAgeToKeepEnabled && (
                                                    <FormInput
                                                        name="minimumEntriesAgeToKeepInMin"
                                                        control={control}
                                                        type="number"
                                                        disabled={formState.isSubmitting}
                                                        addon="minutes"
                                                    />
                                                )}
                                            </div>
                                            <div>
                                                <FormSwitch
                                                    name="isRefreshFrequencyEnabled"
                                                    control={control}
                                                    className="mb-3"
                                                    disabled={
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
                                                        formState.isSubmitting || !formValues.isRefreshFrequencyEnabled
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

function mapToFormData(dto: RevisionsBinConfiguration): RevisionsBinCleanerFormData {
    if (!dto) {
        return {
            isRevisionsBinCleanerEnabled: false,
            isMinimumEntriesAgeToKeepEnabled: false,
            minimumEntriesAgeToKeepInMin: null,
            isRefreshFrequencyEnabled: false,
            refreshFrequencyInSec: null,
        };
    }

    return {
        isRevisionsBinCleanerEnabled: !dto.Disabled,
        isMinimumEntriesAgeToKeepEnabled: dto.MinimumEntriesAgeToKeepInMin != null,
        minimumEntriesAgeToKeepInMin: dto.MinimumEntriesAgeToKeepInMin,
        isRefreshFrequencyEnabled: dto.RefreshFrequencyInSec != null,
        refreshFrequencyInSec: dto.RefreshFrequencyInSec,
    };
}
