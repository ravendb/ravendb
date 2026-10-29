import React, { useEffect } from "react";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { FormDurationPicker, FormInput, FormSwitch } from "components/common/Form";
import FieldLabel from "components/common/FieldLabel";
import OverridableField from "components/common/OverridableField";
import useUniqueId from "components/hooks/useUniqueId";
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

const defaultItemsToProcess = 65536;
const defaultRefreshFrequencyInSec = 60;
const overriddenRefreshFrequencyPlaceholder = { hours: "0", minutes: "0", seconds: "0" };

export default function DocumentRefresh() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const t = useStudioTranslation("documentRefresh");
    const commonT = useStudioTranslation("common");

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

    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });
    const licenseLimitedFrequencyLabelId = useUniqueId("refresh-frequency-label-");

    const isLicenseLimited = minPeriodForRefreshInHours > 0;
    const licenseMinRefreshFrequencyInSec = moment.duration(minPeriodForRefreshInHours, "hours").asSeconds();

    const effectiveRefreshFrequencyInSec =
        isLicenseLimited || formValues.isRefreshFrequencyEnabled
            ? (formValues.refreshFrequency ?? defaultRefreshFrequencyInSec)
            : defaultRefreshFrequencyInSec;

    const refreshFrequencyInHours = moment.duration(effectiveRefreshFrequencyInSec, "seconds").asHours();

    const isLimitWarningVisible =
        isLicenseLimited && formValues.isDocumentRefreshEnabled && refreshFrequencyInHours < minPeriodForRefreshInHours;

    const licenseLimitMessage = t("limitWarning", { hours: minPeriodForRefreshInHours });

    const defaultRefreshFrequency = moment.duration(defaultRefreshFrequencyInSec, "seconds");
    const defaultRefreshFrequencyPlaceholder = {
        hours: t("frequencyPlaceholder", { value: Math.floor(defaultRefreshFrequency.asHours()) }),
        minutes: t("frequencyPlaceholder", { value: defaultRefreshFrequency.minutes() }),
        seconds: t("frequencyPlaceholder", { value: defaultRefreshFrequency.seconds() }),
    };

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            switch (name) {
                case "isDocumentRefreshEnabled": {
                    if (values.isDocumentRefreshEnabled) {
                        setValue("maxItemsToProcess", defaultItemsToProcess, { shouldValidate: true });

                        if (minPeriodForRefreshInHours > 0) {
                            setValue("isRefreshFrequencyEnabled", true, { shouldValidate: true });
                        }
                    } else {
                        setValue("maxItemsToProcess", null, { shouldValidate: true });
                        setValue("isRefreshFrequencyEnabled", false, { shouldValidate: true });
                    }
                    break;
                }
                case "isRefreshFrequencyEnabled": {
                    if (values.isRefreshFrequencyEnabled) {
                        if (values.refreshFrequency == null && minPeriodForRefreshInHours > 0) {
                            setValue("refreshFrequency", licenseMinRefreshFrequencyInSec, { shouldValidate: true });
                        }
                    } else {
                        setValue("refreshFrequency", null, { shouldValidate: true });
                    }
                    break;
                }
            }
        });
        return () => unsubscribe();
    }, [licenseMinRefreshFrequencyInSec, minPeriodForRefreshInHours, setValue, watch]);

    const onSave: SubmitHandler<DocumentRefreshFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("refresh-configuration", "save");

            await databasesService.saveRefreshConfiguration(databaseName, {
                Disabled: !formData.isDocumentRefreshEnabled,
                RefreshFrequencyInSec:
                    isLicenseLimited || formData.isRefreshFrequencyEnabled ? formData.refreshFrequency : null,
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
                            <ConditionalPopover
                                conditions={{ isActive: isLimitWarningVisible, message: licenseLimitMessage }}
                            >
                                <ButtonWithSpinner
                                    type="submit"
                                    variant="primary"
                                    className="mb-3"
                                    icon="save"
                                    disabled={!formState.isDirty || isLimitWarningVisible}
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
                                            >
                                                {t("enableSwitch")}
                                            </FormSwitch>
                                            {isLicenseLimited ? (
                                                <div>
                                                    <FieldLabel id={licenseLimitedFrequencyLabelId} className="d-block">
                                                        {t("frequencyLabel")}
                                                    </FieldLabel>
                                                    <div className="d-inline-block mw-100">
                                                        <div data-testid="refreshFrequencyDurationPicker">
                                                            <FormDurationPicker
                                                                name="refreshFrequency"
                                                                control={control}
                                                                disabled={
                                                                    formState.isSubmitting ||
                                                                    !formValues.isDocumentRefreshEnabled
                                                                }
                                                                aria-labelledby={licenseLimitedFrequencyLabelId}
                                                                placeholder={overriddenRefreshFrequencyPlaceholder}
                                                                showSeconds
                                                            />
                                                        </div>
                                                    </div>
                                                    <div className="d-flex flex-wrap align-items-center column-gap-2 mt-1 small">
                                                        {isLimitWarningVisible && !formState.errors.refreshFrequency ? (
                                                            <span className="text-danger">
                                                                <Icon icon="warning" />
                                                                {licenseLimitMessage}
                                                            </span>
                                                        ) : (
                                                            <span className="text-muted">
                                                                <StudioTrans
                                                                    ns="documentRefresh"
                                                                    i18nKey="licenseMinimum"
                                                                    components={{ strong: <strong /> }}
                                                                    options={{ hours: minPeriodForRefreshInHours }}
                                                                />
                                                            </span>
                                                        )}
                                                        <a href={buyLink} target="_blank">
                                                            {t("compareLicensePlans")}
                                                        </a>
                                                    </div>
                                                </div>
                                            ) : (
                                                <div className="align-self-start mw-100">
                                                    <OverridableField
                                                        control={control}
                                                        overrideName="isRefreshFrequencyEnabled"
                                                        valueName="refreshFrequency"
                                                        setValue={setValue}
                                                        label={t("frequencyLabel")}
                                                        disabled={
                                                            formState.isSubmitting ||
                                                            !formValues.isDocumentRefreshEnabled
                                                        }
                                                    >
                                                        {({ isOverridden, isDisabled, labelId }) => (
                                                            <div data-testid="refreshFrequencyDurationPicker">
                                                                <FormDurationPicker
                                                                    name="refreshFrequency"
                                                                    control={control}
                                                                    disabled={isDisabled}
                                                                    aria-labelledby={labelId}
                                                                    placeholder={
                                                                        isOverridden
                                                                            ? overriddenRefreshFrequencyPlaceholder
                                                                            : defaultRefreshFrequencyPlaceholder
                                                                    }
                                                                    showSeconds
                                                                />
                                                            </div>
                                                        )}
                                                    </OverridableField>
                                                </div>
                                            )}
                                            <OverridableField
                                                control={control}
                                                overrideName="isLimitMaxItemsToProcessEnabled"
                                                valueName="maxItemsToProcess"
                                                setValue={setValue}
                                                optimalValue={defaultItemsToProcess}
                                                optimalValueLabel={defaultItemsToProcess.toLocaleString()}
                                                optimalValueHint={t("maxItemsOptimalHint", {
                                                    items: defaultItemsToProcess.toLocaleString(),
                                                })}
                                                label={t("maxItemsLabel")}
                                                disabled={
                                                    formState.isSubmitting || !formValues.isDocumentRefreshEnabled
                                                }
                                            >
                                                {({ isDisabled }) => (
                                                    <FormInput
                                                        name="maxItemsToProcess"
                                                        control={control}
                                                        type="number"
                                                        placeholder={t("maxItemsPlaceholder")}
                                                        disabled={isDisabled}
                                                        addon={t("itemsAddon")}
                                                    />
                                                )}
                                            </OverridableField>
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
