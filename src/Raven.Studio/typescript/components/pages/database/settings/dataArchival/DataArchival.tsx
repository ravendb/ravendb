import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
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
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormSwitch, DIRTY_AND_VALIDATE } from "components/common/Form";
import FrequencyField from "components/common/FrequencyField";
import MaxItemsToProcessField from "components/common/MaxItemsToProcessField";
import { DEFAULT_ITEMS_TO_PROCESS } from "common/constants/maxItemsToProcess";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useRavenLink } from "hooks/useRavenLink";
import moment from "moment";

const DEFAULT_ARCHIVE_FREQUENCY_IN_SEC = 60;

export default function DataArchival() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDataArchival = useAppSelector(licenseSelectors.statusValue("HasDataArchival"));

    const documentArchiveLinkToOverviewDocs = useRavenLink({ hash: "1FI4YS" });

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

    const resetSettingsOnToggle = (isEnabled: boolean) => {
        setValue("maxItemsToProcess", isEnabled ? DEFAULT_ITEMS_TO_PROCESS : null, DIRTY_AND_VALIDATE);
        if (!isEnabled) {
            setValue("archiveFrequency", null, DIRTY_AND_VALIDATE);
        }
    };

    const onSave: SubmitHandler<DataArchivalFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("data-archival-configuration", "save");

            await databasesService.saveDataArchivalConfiguration(databaseName, {
                Disabled: !formData.isDataArchivalEnabled,
                ArchiveFrequencyInSec: formData.archiveFrequency,
                MaxItemsToProcess: formData.maxItemsToProcess,
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
                            <ConditionalPopover
                                conditions={{
                                    isActive: !hasDataArchival,
                                    message: <FeatureNotAvailableInYourLicensePopoverBody />,
                                }}
                            >
                                <ButtonWithSpinner
                                    type="submit"
                                    variant="primary"
                                    className="mb-3"
                                    icon="save"
                                    disabled={!formState.isDirty || !hasDatabaseAdminAccess}
                                    isSpinning={formState.isSubmitting}
                                >
                                    Save
                                </ButtonWithSpinner>
                            </ConditionalPopover>
                            <Col className={hasDataArchival ? "" : "item-disabled pe-none"}>
                                <Card>
                                    <Card.Body>
                                        <div className="vstack gap-3">
                                            <FormSwitch
                                                name="isDataArchivalEnabled"
                                                control={control}
                                                disabled={formState.isSubmitting}
                                                afterChange={resetSettingsOnToggle}
                                            >
                                                Enable Data Archival
                                            </FormSwitch>
                                            <FrequencyField
                                                control={control}
                                                name="archiveFrequency"
                                                label="Archive frequency"
                                                disabled={!formValues.isDataArchivalEnabled}
                                                defaultFrequencyInSec={DEFAULT_ARCHIVE_FREQUENCY_IN_SEC}
                                            />
                                            <MaxItemsToProcessField
                                                control={control}
                                                name="maxItemsToProcess"
                                                disabled={!formValues.isDataArchivalEnabled}
                                            />
                                        </div>
                                    </Card.Body>
                                </Card>
                            </Col>
                        </Form>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored defaultOpen={hasDataArchival ? null : "licensing"}>
                            <AccordionItemWrapper targetId="about" icon="about" color="info">
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
                                <p>Example of a document scheduled for archival:</p>
                                <Code code={codeExample} language="javascript" />
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href={documentArchiveLinkToOverviewDocs} target="_blank">
                                    <Icon icon="newtab" /> Docs - Data Archival - Overview
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
  "Example": "Set a timestamp in the @archive-at metadata property",
  "@metadata": {
    "@collection": "Foo",
    "@archive-at": "${moment().add(1, "year").toISOString()}"
  }
}`;

function mapToFormData(dto: DataArchivalConfiguration): DataArchivalFormData {
    if (!dto) {
        return {
            isDataArchivalEnabled: false,
            archiveFrequency: null,
            maxItemsToProcess: null,
        };
    }

    return {
        isDataArchivalEnabled: !dto.Disabled,
        archiveFrequency: dto.ArchiveFrequencyInSec ?? null,
        maxItemsToProcess: dto.MaxItemsToProcess ?? null,
    };
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Data Archival",
        featureIcon: "data-archival",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
        quill: { value: true },
    },
];
