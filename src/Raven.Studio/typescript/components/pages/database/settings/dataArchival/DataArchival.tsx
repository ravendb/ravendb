import React, { useEffect } from "react";
import { Card, CardBody, Col, Form, Row } from "reactstrap";
import { useServices } from "hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import {
    DataArchivalFormData,
    dataArchivalYupResolver,
} from "components/pages/database/settings/dataArchival/DataArchivalValidation";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import { useEventsCollector } from "hooks/useEventsCollector";
import DataArchivalConfiguration = Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration;
import { tryHandleSubmit } from "components/utils/common";
import messagePublisher from "common/messagePublisher";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { todo } from "common/developmentHelper";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput, FormSwitch } from "components/common/Form";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import FeatureNotAvailableInYourLicensePopover from "components/common/FeatureNotAvailableInYourLicensePopover";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

export default function DataArchival() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDataArchival = useAppSelector(licenseSelectors.statusValue("HasDataArchival"));

    const { databasesService } = useServices();
    const asyncGetDataArchivalConfiguration = useAsyncCallback<DataArchivalFormData>(async () =>
        mapToFormData(await databasesService.getDataArchivalConfiguration(databaseName))
    );
    const { handleSubmit, control, formState, reset, setValue } = useForm<DataArchivalFormData>({
        resolver: dataArchivalYupResolver,
        mode: "all",
        defaultValues: asyncGetDataArchivalConfiguration.execute,
    });
    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control: control });
    const { reportEvent } = useEventsCollector();

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasDataArchival,
            },
        ],
    });

    useEffect(() => {
        if (!formValues.isArchiveFrequencyEnabled && formValues.archiveFrequency !== null) {
            setValue("archiveFrequency", null, { shouldValidate: true });
        }
        if (!formValues.isDataArchivalEnabled && formValues.isArchiveFrequencyEnabled) {
            setValue("isArchiveFrequencyEnabled", false, { shouldValidate: true });
        }
    }, [formValues.isDataArchivalEnabled, formValues.isArchiveFrequencyEnabled, formValues.archiveFrequency, setValue]);

    const onSave: SubmitHandler<DataArchivalFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("data-archival-configuration", "save");

            await databasesService.saveDataArchivalConfiguration(databaseName, {
                Disabled: !formData.isDataArchivalEnabled,
                ArchiveFrequencyInSec: formData.isArchiveFrequencyEnabled ? formData.archiveFrequency : null,
            });

            messagePublisher.reportSuccess("Data archival configuration saved successfully");
            activeDatabaseTracker.default.database().hasArchivalConfiguration(formData.isDataArchivalEnabled);
            reset(formData);
        });
    };

    if (
        asyncGetDataArchivalConfiguration.status === "not-requested" ||
        asyncGetDataArchivalConfiguration.status === "loading"
    ) {
        return <LoadingView />;
    }

    if (asyncGetDataArchivalConfiguration.status === "error") {
        return <LoadError error="Unable to load data archival" refresh={asyncGetDataArchivalConfiguration.execute} />;
    }

    todo("Feature", "Damian", "Render you do not have permission to this view");

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                            <AboutViewHeading
                                title="Data Archival"
                                icon="data-archival"
                                licenseBadgeText={hasDataArchival ? null : "Enterprise"}
                            />
                            <div id="saveDataArchival" className="w-fit-content">
                                <ButtonWithSpinner
                                    type="submit"
                                    color="primary"
                                    className="mb-3"
                                    icon="save"
                                    disabled={!formState.isDirty || !hasDatabaseAdminAccess}
                                    isSpinning={formState.isSubmitting}
                                >
                                    Save
                                </ButtonWithSpinner>
                            </div>
                            {!hasDataArchival && <FeatureNotAvailableInYourLicensePopover target="saveDataArchival" />}
                            <Col className={hasDataArchival ? "" : "item-disabled pe-none"}>
                                <Card>
                                    <CardBody>
                                        <div className="vstack gap-2">
                                            <FormSwitch
                                                name="isDataArchivalEnabled"
                                                control={control}
                                                disabled={formState.isSubmitting}
                                            >
                                                Enable Data Archival
                                            </FormSwitch>
                                            <div>
                                                <FormSwitch
                                                    name="isArchiveFrequencyEnabled"
                                                    control={control}
                                                    className="mb-3"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isDataArchivalEnabled
                                                    }
                                                >
                                                    Set custom archive frequency
                                                </FormSwitch>
                                                <FormInput
                                                    name="archiveFrequency"
                                                    control={control}
                                                    type="number"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isArchiveFrequencyEnabled
                                                    }
                                                    placeholder="Default (60)"
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
                        <AboutViewAnchored defaultOpen={hasDataArchival ? null : "licensing"}>
                            <AccordionItemWrapper
                                targetId="about"
                                icon="about"
                                color="info"
                                description="Get additional info on this feature"
                                heading="About this view"
                            >
                                <p>
                                    When <strong>Data Archival</strong> is enabled:
                                </p>
                                <ul>
                                    <li>
                                        The server scans the database at the specified <strong>frequency</strong>,
                                        searching for documents that should be archived.
                                    </li>
                                    <li>
                                        Any document that has an <code>@archive-at</code> metadata property whose time
                                        has passed at the time of the scan will be archived:
                                        <ul>
                                            <li>The archived document will be compressed</li>
                                            <li>
                                                The <code>@archive-at</code> metadata property will be replaced by:{" "}
                                                <code>@archived: true</code>
                                            </li>
                                            <li>
                                                Per-index/subscription, you can configure whether archived documents
                                                will be included in the indexing and subscription processes
                                            </li>
                                        </ul>
                                    </li>
                                </ul>

                                <p>Sample document:</p>
                                <Code code={codeExample} language="javascript" />
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href="#" target="_blank">
                                    <Icon icon="newtab" /> Docs - Data Archival
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={hasDataArchival}
                                data={featureAvailability}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

const codeExample = `{
  "Example": 
    "Set a timestamp in the @archive-at metadata property",
  "@metadata": {
    "@collection": "Foo",
    "@archive-at": "2023-07-16T08:00:00.0000000Z"
  }
}`;

function mapToFormData(dto: DataArchivalConfiguration): DataArchivalFormData {
    if (!dto) {
        return {
            isDataArchivalEnabled: false,
            isArchiveFrequencyEnabled: false,
            archiveFrequency: null,
        };
    }

    return {
        isDataArchivalEnabled: !dto.Disabled,
        isArchiveFrequencyEnabled: dto.ArchiveFrequencyInSec != null,
        archiveFrequency: dto.ArchiveFrequencyInSec,
    };
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Data Archival",
        featureIcon: "data-archival",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
