import React from "react";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "hooks/useServices";
import Collapse from "react-bootstrap/Collapse";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
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
import { FormDurationPicker, FormGroup, FormSwitch, DIRTY_AND_VALIDATE } from "components/common/Form";
import { ZERO_DURATION_PLACEHOLDER } from "components/common/DurationPicker";
import moment from "moment";
import FrequencyField from "components/common/FrequencyField";
import { RevisionsBinCleanerInfoHub } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerInfoHub";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import {
    DEFAULT_CLEANER_FREQUENCY_IN_SEC,
    revisionsBinCleanerUtils,
} from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerUtils";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Form from "react-bootstrap/Form";

export default function RevisionsBinCleaner() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const asyncGetRevisionsBinCleanerConfiguration = useAsyncCallback<RevisionsBinCleanerFormData>(async () =>
        revisionsBinCleanerUtils.mapToFormData(await databasesService.getRevisionsBinCleanerConfiguration(databaseName))
    );

    const { handleSubmit, control, formState, reset, setValue, getValues } = useForm<
        Partial<RevisionsBinCleanerFormData>
    >({
        resolver: revisionsBinCleanerYupResolver,
        mode: "all",
        defaultValues: asyncGetRevisionsBinCleanerConfiguration.execute,
    });

    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control });

    const { reportEvent } = useEventsCollector();

    const resetSettingsOnToggle = (isEnabled: boolean) => {
        if (isEnabled) {
            return;
        }

        setValue("isMinimumEntriesAgeToKeepEnabled", false, DIRTY_AND_VALIDATE);
        setValue("minimumEntriesAgeToKeep", null, DIRTY_AND_VALIDATE);
        setValue("cleanerFrequencyInSec", null, DIRTY_AND_VALIDATE);
    };

    const toggleMinimumEntriesAgeToKeep = (isEnabled: boolean) => {
        if (!isEnabled) {
            setValue("minimumEntriesAgeToKeep", null, DIRTY_AND_VALIDATE);
        } else if (getValues("minimumEntriesAgeToKeep") == null) {
            setValue("minimumEntriesAgeToKeep", moment.duration(1, "month").asSeconds(), DIRTY_AND_VALIDATE);
        }
    };

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
                                    variant="primary"
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
                                    <Card.Body>
                                        <FormGroup>
                                            <FormSwitch
                                                name="isRevisionsBinCleanerEnabled"
                                                disabled={!hasDatabaseAdminAccess}
                                                control={control}
                                                afterChange={resetSettingsOnToggle}
                                            >
                                                Enable Revisions Bin Cleaner
                                            </FormSwitch>
                                        </FormGroup>
                                        <FormGroup className="d-flex gap-2 align-items-center">
                                            <FormSwitch
                                                name="isMinimumEntriesAgeToKeepEnabled"
                                                control={control}
                                                color="primary"
                                                afterChange={toggleMinimumEntriesAgeToKeep}
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
                                                    <PopoverWithHoverWrapper message="All items in the Revisions Bin will be deleted when 'Set minimum entries age to keep' is toggled off.">
                                                        <Icon margin="m-0" icon="warning" color="warning" />
                                                    </PopoverWithHoverWrapper>
                                                )}
                                        </FormGroup>
                                        <Collapse
                                            data-testid="collapse"
                                            appear
                                            in={
                                                formValues.isMinimumEntriesAgeToKeepEnabled &&
                                                formValues.isRevisionsBinCleanerEnabled
                                            }
                                        >
                                            <div>
                                                <FormGroup data-testid="durationPicker">
                                                    <FormDurationPicker
                                                        control={control}
                                                        disabled={
                                                            !hasDatabaseAdminAccess ||
                                                            formState.isSubmitting ||
                                                            !formValues.isMinimumEntriesAgeToKeepEnabled
                                                        }
                                                        placeholder={ZERO_DURATION_PLACEHOLDER}
                                                        name="minimumEntriesAgeToKeep"
                                                        showDays
                                                    />
                                                </FormGroup>
                                            </div>
                                        </Collapse>
                                        <FrequencyField
                                            control={control}
                                            name="cleanerFrequencyInSec"
                                            label="Cleaner frequency"
                                            disabled={
                                                !hasDatabaseAdminAccess || !formValues.isRevisionsBinCleanerEnabled
                                            }
                                            defaultFrequencyInSec={DEFAULT_CLEANER_FREQUENCY_IN_SEC}
                                        />
                                    </Card.Body>
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
