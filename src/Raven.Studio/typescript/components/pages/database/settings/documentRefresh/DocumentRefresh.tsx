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
import moment from "moment";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

const DEFAULT_REFRESH_FREQUENCY_IN_SEC = 60;

export default function DocumentRefresh() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const t = useStudioTranslation("documentRefresh");
    const commonT = useStudioTranslation("common");

    const asyncGetRefreshConfiguration = useAsyncCallback<DocumentRefreshFormData>(async () =>
        mapToFormData(await databasesService.getRefreshConfiguration(databaseName))
    );
    const { handleSubmit, control, formState, reset, setValue, getValues } = useForm<DocumentRefreshFormData>({
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

    const licenseLimit = getLicenseFrequencyLimit({
        minPeriodInHours: minPeriodForRefreshInHours,
        isFeatureEnabled: formValues.isDocumentRefreshEnabled,
        frequencyInSec: formValues.refreshFrequency,
        defaultFrequencyInSec: DEFAULT_REFRESH_FREQUENCY_IN_SEC,
        limitMessage: t("limitWarning", { count: minPeriodForRefreshInHours }),
    });

    const resetSettingsOnToggle = (isEnabled: boolean) => {
        if (!isEnabled) {
            setValue("maxItemsToProcess", null, DIRTY_AND_VALIDATE);
            setValue("refreshFrequency", null, DIRTY_AND_VALIDATE);
            return;
        }

        setValue("maxItemsToProcess", DEFAULT_ITEMS_TO_PROCESS, DIRTY_AND_VALIDATE);
        if (licenseLimit.isLicenseLimited && getValues("refreshFrequency") == null) {
            setValue("refreshFrequency", licenseLimit.minFrequencyInSec, DIRTY_AND_VALIDATE);
        }
    };

    const onSave: SubmitHandler<DocumentRefreshFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("refresh-configuration", "save");

            await databasesService.saveRefreshConfiguration(databaseName, {
                Disabled: !formData.isDocumentRefreshEnabled,
                RefreshFrequencyInSec: formData.refreshFrequency,
                MaxItemsToProcess: formData.maxItemsToProcess,
            });

            messagePublisher.reportSuccess(t("saveSuccess"));
            activeDatabaseTracker.default.database().hasRefreshConfiguration(formData.isDocumentRefreshEnabled);

            reset(formData);
        });
    };

    if (asyncGetRefreshConfiguration.status === "not-requested" || asyncGetRefreshConfiguration.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGetRefreshConfiguration.status === "error") {
        return <LoadError error={t("loadError")} refresh={asyncGetRefreshConfiguration.execute} />;
    }

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                            <AboutViewHeading title={t("title")} icon="expos-refresh" />
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
                                    {commonT("save")}
                                </ButtonWithSpinner>
                            </ConditionalPopover>
                            <Col>
                                <Card>
                                    <Card.Body>
                                        <div className="vstack gap-3">
                                            <FormSwitch
                                                name="isDocumentRefreshEnabled"
                                                control={control}
                                                disabled={formState.isSubmitting}
                                                afterChange={resetSettingsOnToggle}
                                            >
                                                {t("enableSwitch")}
                                            </FormSwitch>
                                            <FrequencyField
                                                control={control}
                                                name="refreshFrequency"
                                                label={t("frequencyLabel")}
                                                disabled={!formValues.isDocumentRefreshEnabled}
                                                defaultFrequencyInSec={DEFAULT_REFRESH_FREQUENCY_IN_SEC}
                                                licenseLimit={licenseLimit}
                                            />
                                            <MaxItemsToProcessField
                                                control={control}
                                                name="maxItemsToProcess"
                                                disabled={!formValues.isDocumentRefreshEnabled}
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
                                    <StudioTrans
                                        ns="documentRefresh"
                                        i18nKey="about.intro"
                                        components={{ strong: <strong /> }}
                                    />
                                </p>
                                <ul>
                                    <li>
                                        <StudioTrans
                                            ns="documentRefresh"
                                            i18nKey="about.scanItem"
                                            components={{ strong: <strong /> }}
                                        />
                                    </li>
                                    <li>
                                        <StudioTrans
                                            ns="documentRefresh"
                                            i18nKey="about.metadataItem"
                                            components={{ code: <code /> }}
                                        />
                                    </li>
                                    <li>{t("about.triggerItem")}</li>
                                </ul>
                                <p>{t("about.exampleLabel")}</p>
                                <Code code={codeExample} language="javascript" />
                                <hr />
                                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                                <a href={documentRefreshDocsLink} target="_blank">
                                    <Icon icon="newtab" /> {t("about.docsLink")}
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={!minPeriodForRefreshInHours}
                                isOpenedByDefault={false}
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
            refreshFrequency: null,
            maxItemsToProcess: null,
        };
    }

    return {
        isDocumentRefreshEnabled: !dto.Disabled,
        refreshFrequency: dto.RefreshFrequencyInSec ?? null,
        maxItemsToProcess: dto.MaxItemsToProcess ?? null,
    };
}

const codeExample = `{
  "Example": 
    "Set a timestamp in the @refresh metadata property",
  "@metadata": {
    "@collection": "Foo",
    "@refresh": "${moment().add(1, "year").toISOString()}"
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
