import React, { useEffect } from "react";
import { Card, CardBody, Col, Form, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { FormInput, FormSwitch } from "components/common/Form";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { tryHandleSubmit } from "components/utils/common";
import { DocumentRefreshFormData, documentRefreshYupResolver } from "./DocumentRefreshValidation";
import Code from "components/common/Code";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import ServerRefreshConfiguration = Raven.Client.Documents.Operations.Refresh.RefreshConfiguration;
import messagePublisher = require("common/messagePublisher");
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import moment = require("moment");
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import RichAlert from "components/common/RichAlert";

const defaultItemsToProcess = 65536;

export default function DocumentRefresh() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();

    const asyncGetRefreshConfiguration = useAsyncCallback<DocumentRefreshFormData>(async () =>
        mapToFormData(await databasesService.getRefreshConfiguration(databaseName))
    );
    const { handleSubmit, control, formState, reset, setValue, watch } = useForm<DocumentRefreshFormData>({
        resolver: documentRefreshYupResolver,
        mode: "all",
        defaultValues: asyncGetRefreshConfiguration.execute,
    });

    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control: control });
    const { reportEvent } = useEventsCollector();

    const documentRefreshDocsLink = useRavenLink({ hash: "1PKUYJ" });

    const minPeriodForRefreshInHours = useAppSelector(licenseSelectors.statusValue("MinPeriodForRefreshInHours"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: minPeriodForRefreshInHours,
            },
        ],
    });

    const refreshFrequencyInHours = moment.duration(formValues.refreshFrequency, "seconds").asHours();

    const isLimitWarningVisible =
        minPeriodForRefreshInHours > 0 &&
        formValues.isRefreshFrequencyEnabled &&
        refreshFrequencyInHours < minPeriodForRefreshInHours;

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            switch (name) {
                case "isDocumentRefreshEnabled": {
                    if (values.isDocumentRefreshEnabled) {
                        setValue("isLimitMaxItemsToProcessEnabled", true, { shouldValidate: true });
                    } else {
                        setValue("isLimitMaxItemsToProcessEnabled", false, { shouldValidate: true });
                        setValue("isRefreshFrequencyEnabled", false, { shouldValidate: true });
                    }
                    break;
                }
                case "isLimitMaxItemsToProcessEnabled": {
                    if (values.isLimitMaxItemsToProcessEnabled) {
                        setValue("maxItemsToProcess", defaultItemsToProcess, { shouldValidate: true });
                    } else {
                        setValue("maxItemsToProcess", null, { shouldValidate: true });
                    }
                    break;
                }
                case "isRefreshFrequencyEnabled": {
                    if (!values.isRefreshFrequencyEnabled) {
                        setValue("refreshFrequency", null, { shouldValidate: true });
                    }
                    break;
                }
            }
        });
        return () => unsubscribe();
    }, [setValue, watch]);

    const onSave: SubmitHandler<DocumentRefreshFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("refresh-configuration", "save");

            await databasesService.saveRefreshConfiguration(databaseName, {
                Disabled: !formData.isDocumentRefreshEnabled,
                RefreshFrequencyInSec: formData.isRefreshFrequencyEnabled ? formData.refreshFrequency : null,
                MaxItemsToProcess: formData.isLimitMaxItemsToProcessEnabled ? formData.maxItemsToProcess : null,
            });

            messagePublisher.reportSuccess("Refresh configuration saved successfully");
            activeDatabaseTracker.default.database().hasRefreshConfiguration(formData.isDocumentRefreshEnabled);

            reset(formData);
        });
    };

    if (asyncGetRefreshConfiguration.status === "not-requested" || asyncGetRefreshConfiguration.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGetRefreshConfiguration.status === "error") {
        return <LoadError error="Unable to load document refresh" refresh={asyncGetRefreshConfiguration.execute} />;
    }

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                            <AboutViewHeading title="Document Refresh" icon="expos-refresh" />
                            <ButtonWithSpinner
                                type="submit"
                                color="primary"
                                className="mb-3"
                                icon="save"
                                disabled={!formState.isDirty || isLimitWarningVisible}
                                isSpinning={formState.isSubmitting}
                            >
                                Save
                            </ButtonWithSpinner>
                            <Col>
                                <Card>
                                    <CardBody>
                                        <div className="vstack gap-2">
                                            <FormSwitch
                                                name="isDocumentRefreshEnabled"
                                                control={control}
                                                disabled={formState.isSubmitting}
                                            >
                                                Enable Document Refresh
                                            </FormSwitch>
                                            <div>
                                                <FormSwitch
                                                    name="isRefreshFrequencyEnabled"
                                                    control={control}
                                                    className="mb-3"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isDocumentRefreshEnabled
                                                    }
                                                >
                                                    Set custom refresh frequency
                                                </FormSwitch>
                                                <FormInput
                                                    name="refreshFrequency"
                                                    control={control}
                                                    type="number"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isRefreshFrequencyEnabled
                                                    }
                                                    placeholder={
                                                        minPeriodForRefreshInHours > 0
                                                            ? `Default (${moment
                                                                  .duration(minPeriodForRefreshInHours, "hours")
                                                                  .asSeconds()})`
                                                            : "Default (60)"
                                                    }
                                                    addon="seconds"
                                                />
                                                {isLimitWarningVisible && (
                                                    <RichAlert variant="warning" className="mt-3">
                                                        Your current license does not allow a frequency higher than{" "}
                                                        {minPeriodForRefreshInHours} hours (
                                                        {moment
                                                            .duration(minPeriodForRefreshInHours, "hours")
                                                            .asSeconds()}{" "}
                                                        seconds)
                                                    </RichAlert>
                                                )}
                                            </div>
                                            <div>
                                                <FormSwitch
                                                    name="isLimitMaxItemsToProcessEnabled"
                                                    control={control}
                                                    className="mb-3"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isDocumentRefreshEnabled
                                                    }
                                                >
                                                    Set max number of documents to process in a single run
                                                </FormSwitch>
                                                <FormInput
                                                    name="maxItemsToProcess"
                                                    control={control}
                                                    type="number"
                                                    disabled={
                                                        formState.isSubmitting ||
                                                        !formValues.isLimitMaxItemsToProcessEnabled
                                                    }
                                                    addon="items"
                                                />
                                            </div>
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>
                        </Form>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper
                                targetId="1"
                                icon="about"
                                color="info"
                                description="Get additional info on this feature"
                                heading="About this view"
                            >
                                <p>
                                    When <strong>Document Refresh</strong> is enabled:
                                </p>
                                <ul>
                                    <li>
                                        The server scans the database at the specified <strong>frequency</strong>,
                                        searching for documents that should be refreshed.
                                    </li>
                                    <li>
                                        Any document that has a <code>@refresh</code> metadata property whose time has
                                        passed at the time of the scan will be modified by removing this property.
                                    </li>
                                    <li>
                                        This modification will trigger any processes related to the document, such as:
                                        re-indexing or taking part in an ongoing-task (e.g. Replication, ETL,
                                        Subscriptions, etc.), as defined by your configuration.
                                    </li>
                                </ul>
                                <p>Sample document:</p>
                                <Code code={codeExample} language="javascript" />
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href={documentRefreshDocsLink} target="_blank">
                                    <Icon icon="newtab" /> Docs - Document Refresh
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={!minPeriodForRefreshInHours}
                                data={featureAvailability}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

function mapToFormData(dto: ServerRefreshConfiguration): DocumentRefreshFormData {
    if (!dto) {
        return {
            isDocumentRefreshEnabled: false,
            isRefreshFrequencyEnabled: false,
            refreshFrequency: null,
            isLimitMaxItemsToProcessEnabled: false,
            maxItemsToProcess: null,
        };
    }

    return {
        isDocumentRefreshEnabled: !dto.Disabled,
        isRefreshFrequencyEnabled: dto.RefreshFrequencyInSec != null,
        refreshFrequency: dto.RefreshFrequencyInSec,
        isLimitMaxItemsToProcessEnabled: dto.MaxItemsToProcess != null,
        maxItemsToProcess: dto.MaxItemsToProcess,
    };
}

const codeExample = `{
  "Example": 
    "Set a timestamp in the @refresh metadata property",
  "@metadata": {
    "@collection": "Foo",
    "@refresh": "2023-07-16T08:00:00.0000000Z"
  }
}`;

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Min check frequency (hrs)",
        featureIcon: "clock",
        community: { value: 36 },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
];
