import React, { useEffect } from "react";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
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
import moment from "moment";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import RichAlert from "components/common/RichAlert";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

const defaultItemsToProcess = 65536;

export default function DocumentRefresh() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const { t } = useStudioTranslation("documentRefresh");
    const { t: tCommon } = useStudioTranslation("common");

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
    const minPeriodForRefreshInSeconds = moment.duration(minPeriodForRefreshInHours, "hours").asSeconds();

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
                            <ButtonWithSpinner
                                type="submit"
                                variant="primary"
                                className="mb-3"
                                icon="save"
                                disabled={!formState.isDirty || isLimitWarningVisible}
                                isSpinning={formState.isSubmitting}
                            >
                                {tCommon("save")}
                            </ButtonWithSpinner>
                            <Col>
                                <Card>
                                    <Card.Body>
                                        <div className="vstack gap-2">
                                            <FormSwitch
                                                name="isDocumentRefreshEnabled"
                                                control={control}
                                                disabled={formState.isSubmitting}
                                            >
                                                {t("enableSwitch")}
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
                                                    {t("customFrequencySwitch")}
                                                </FormSwitch>
                                                <FormInput
                                                    name="refreshFrequency"
                                                    control={control}
                                                    type="number"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isRefreshFrequencyEnabled
                                                    }
                                                    placeholder={t("frequencyPlaceholder", {
                                                        seconds:
                                                            minPeriodForRefreshInHours > 0
                                                                ? minPeriodForRefreshInSeconds
                                                                : 60,
                                                    })}
                                                    addon={t("secondsAddon")}
                                                />
                                                {isLimitWarningVisible && (
                                                    <RichAlert variant="warning" className="mt-3">
                                                        {t("limitWarning", {
                                                            hours: minPeriodForRefreshInHours,
                                                            seconds: minPeriodForRefreshInSeconds,
                                                        })}
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
                                                    {t("maxItemsSwitch")}
                                                </FormSwitch>
                                                <FormInput
                                                    name="maxItemsToProcess"
                                                    control={control}
                                                    type="number"
                                                    disabled={
                                                        formState.isSubmitting ||
                                                        !formValues.isLimitMaxItemsToProcessEnabled
                                                    }
                                                    addon={t("itemsAddon")}
                                                />
                                            </div>
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
                                <div className="small-label mb-2">{tCommon("usefulLinks")}</div>
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
