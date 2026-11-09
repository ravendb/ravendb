import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { FormSwitch, DIRTY_AND_VALIDATE } from "components/common/Form";
import FrequencyField, { getLicenseFrequencyLimit } from "components/common/FrequencyField";
import MaxItemsToProcessField from "components/common/MaxItemsToProcessField";
import { DEFAULT_ITEMS_TO_PROCESS } from "common/constants/maxItemsToProcess";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { tryHandleSubmit } from "components/utils/common";
import { DocumentExpirationFormData, documentExpirationYupResolver } from "./DocumentExpirationValidation";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import messagePublisher from "common/messagePublisher";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import Code from "components/common/Code";
import { useAsyncCallback } from "react-async-hook";
import ServerExpirationConfiguration = Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration;
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import moment from "moment";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import { ConditionalPopover } from "components/common/ConditionalPopover";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";

const DEFAULT_DELETE_FREQUENCY_IN_SEC = 60;

export default function DocumentExpiration() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();

    const asyncGetExpirationConfiguration = useAsyncCallback<DocumentExpirationFormData>(async () =>
        mapToFormData(await databasesService.getExpirationConfiguration(databaseName))
    );

    const { handleSubmit, control, formState, reset, setValue, getValues } = useForm<DocumentExpirationFormData>({
        resolver: documentExpirationYupResolver,
        mode: "all",
        defaultValues: asyncGetExpirationConfiguration.execute,
    });

    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control: control });

    const { reportEvent } = useEventsCollector();

    const documentExpirationDocsLink = useRavenLink({ hash: "XBFEKZ" });

    const minPeriodForExpirationInHours = useAppSelector(licenseSelectors.statusValue("MinPeriodForExpirationInHours"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: minPeriodForExpirationInHours,
            },
        ],
    });

    const licenseLimit = getLicenseFrequencyLimit({
        minPeriodInHours: minPeriodForExpirationInHours,
        isFeatureEnabled: formValues.isDocumentExpirationEnabled,
        frequencyInSec: formValues.deleteFrequency,
        defaultFrequencyInSec: DEFAULT_DELETE_FREQUENCY_IN_SEC,
        limitMessage:
            minPeriodForExpirationInHours > 0
                ? `Your license doesn't allow an expiration frequency below ${pluralizeHelpers.pluralize(minPeriodForExpirationInHours, "hour", "hours")}.`
                : null,
    });

    const resetSettingsOnToggle = (isEnabled: boolean) => {
        if (!isEnabled) {
            setValue("maxItemsToProcess", null, DIRTY_AND_VALIDATE);
            setValue("deleteFrequency", null, DIRTY_AND_VALIDATE);
            return;
        }

        setValue("maxItemsToProcess", DEFAULT_ITEMS_TO_PROCESS, DIRTY_AND_VALIDATE);
        if (licenseLimit.isLicenseLimited && getValues("deleteFrequency") == null) {
            setValue("deleteFrequency", licenseLimit.minFrequencyInSec, DIRTY_AND_VALIDATE);
        }
    };

    const onSave: SubmitHandler<DocumentExpirationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("expiration-configuration", "save");

            await databasesService.saveExpirationConfiguration(databaseName, {
                Disabled: !formData.isDocumentExpirationEnabled,
                DeleteFrequencyInSec: formData.deleteFrequency,
                MaxItemsToProcess: formData.maxItemsToProcess,
            });

            messagePublisher.reportSuccess("Expiration configuration saved successfully");
            activeDatabaseTracker.default.database().hasExpirationConfiguration(formData.isDocumentExpirationEnabled);

            reset(formData);
        });
    };

    if (
        asyncGetExpirationConfiguration.status === "not-requested" ||
        asyncGetExpirationConfiguration.status === "loading"
    ) {
        return <LoadingView />;
    }

    if (asyncGetExpirationConfiguration.status === "error") {
        return (
            <LoadError error="Unable to load document expiration" refresh={asyncGetExpirationConfiguration.execute} />
        );
    }

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                            <AboutViewHeading title="Document Expiration" icon="document-expiration" />
                            <ConditionalPopover
                                conditions={{ isActive: licenseLimit.isBelowLimit, message: licenseLimit.limitMessage }}
                            >
                                <ButtonWithSpinner
                                    type="submit"
                                    variant="primary"
                                    className="mb-3"
                                    icon="save"
                                    disabled={!formState.isDirty || licenseLimit.isBelowLimit}
                                    isSpinning={formState.isSubmitting}
                                >
                                    Save
                                </ButtonWithSpinner>
                            </ConditionalPopover>
                            <Col>
                                <Card>
                                    <Card.Body>
                                        <div className="vstack gap-3">
                                            <FormSwitch
                                                name="isDocumentExpirationEnabled"
                                                control={control}
                                                afterChange={resetSettingsOnToggle}
                                            >
                                                Enable Document Expiration
                                            </FormSwitch>
                                            <FrequencyField
                                                control={control}
                                                name="deleteFrequency"
                                                label="Expiration frequency"
                                                disabled={!formValues.isDocumentExpirationEnabled}
                                                defaultFrequencyInSec={DEFAULT_DELETE_FREQUENCY_IN_SEC}
                                                licenseLimit={licenseLimit}
                                            />
                                            <MaxItemsToProcessField
                                                control={control}
                                                name="maxItemsToProcess"
                                                disabled={!formValues.isDocumentExpirationEnabled}
                                            />
                                        </div>
                                    </Card.Body>
                                </Card>
                            </Col>
                        </Form>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper targetId="1" icon="about" color="info">
                                <p>
                                    When <strong>Document Expiration</strong> is enabled:
                                </p>
                                <ul>
                                    <li>
                                        The server scans the database at the specified <strong>frequency</strong>,
                                        searching for documents that should be deleted.
                                    </li>
                                    <li>
                                        Any document that has an <code>@expires</code> metadata property whose time has
                                        passed at the time of the scan will be removed.
                                    </li>
                                </ul>

                                <p>Example of a document scheduled for expiration:</p>
                                <Code code={codeExample} language="javascript" />
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href={documentExpirationDocsLink} target="_blank">
                                    <Icon icon="newtab" /> Docs - Document Expiration
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={!minPeriodForExpirationInHours}
                                data={featureAvailability}
                                isOpenedByDefault={false}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

function mapToFormData(dto: ServerExpirationConfiguration): DocumentExpirationFormData {
    if (!dto) {
        return {
            isDocumentExpirationEnabled: false,
            deleteFrequency: null,
            maxItemsToProcess: null,
        };
    }

    return {
        isDocumentExpirationEnabled: !dto.Disabled,
        deleteFrequency: dto.DeleteFrequencyInSec ?? null,
        maxItemsToProcess: dto.MaxItemsToProcess ?? null,
    };
}

const codeExample = `{
    "Example": 
      "Set a timestamp in the @expires metadata property",
    "@metadata": {
      "@collection": "Foo",
      "@expires": "${moment().add(1, "year").toISOString()}"
    }
}`;

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Min check frequency (hrs)",
        featureIcon: "clock",
        community: { value: 36 },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
        quill: { value: Infinity },
    },
];
