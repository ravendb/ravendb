import React from "react";
import { Card, CardBody, Col, Form, Label, PopoverBody, Row, UncontrolledPopover } from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormInput, FormSelect, FormSwitch } from "components/common/Form";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import {
    StudioGlobalConfigurationFormData,
    studioGlobalConfigurationYupResolver,
} from "./StudioGlobalConfigurationValidation";
import studioSettings = require("common/settings/studioSettings");
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { studioEnvironmentOptions } from "components/common/studioConfiguration/StudioConfigurationUtils";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import FeatureNotAvailableInYourLicensePopover from "components/common/FeatureNotAvailableInYourLicensePopover";

export default function StudioGlobalConfiguration() {
    const asyncGlobalSettings = useAsyncCallback<StudioGlobalConfigurationFormData>(async () => {
        const settings = await studioSettings.default.globalSettings(true);

        return {
            environment: settings.environment.getValue(),
            replicationFactor: settings.replicationFactor.getValue(),
            isCollapseDocsWhenOpening: settings.collapseDocsWhenOpening.getValue(),
            isSendUsageStats: settings.sendUsageStats.getValue(),
        };
    });

    const { handleSubmit, control, formState, reset } = useForm<StudioGlobalConfigurationFormData>({
        resolver: studioGlobalConfigurationYupResolver,
        mode: "all",
        defaultValues: asyncGlobalSettings.execute,
    });

    useDirtyFlag(formState.isDirty);

    const clientConfigurationDocsLink = useRavenLink({ hash: "HIR1VP" });
    const { reportEvent } = useEventsCollector();

    const hasStudioConfiguration = useAppSelector(licenseSelectors.statusValue("HasStudioConfiguration"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasStudioConfiguration,
            },
        ],
    });

    const onSave: SubmitHandler<StudioGlobalConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("studio-configuration-global", "save");
            const settings = await studioSettings.default.globalSettings();

            settings.environment.setValueLazy(formData.environment);
            settings.replicationFactor.setValueLazy(formData.replicationFactor);
            settings.collapseDocsWhenOpening.setValue(formData.isCollapseDocsWhenOpening);
            settings.sendUsageStats.setValueLazy(formData.isSendUsageStats);

            await settings.save();
            reset(formData);
        });
    };

    const onRefresh = async () => {
        reset(await asyncGlobalSettings.execute());
    };

    if (asyncGlobalSettings.status === "not-requested" || asyncGlobalSettings.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGlobalSettings.status === "error") {
        return <LoadError error="Unable to load studio configuration" refresh={onRefresh} />;
    }

    return (
        <div className="content-margin">
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading
                        icon="studio-configuration"
                        title="Studio Configuration"
                        licenseBadgeText={hasStudioConfiguration ? null : "Professional +"}
                    />
                    <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                        <div id="saveStudioConfiguration" className="w-fit-content">
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
                        </div>
                        {!hasStudioConfiguration && (
                            <FeatureNotAvailableInYourLicensePopover target="saveStudioConfiguration" />
                        )}
                        <div className={hasStudioConfiguration ? null : "item-disabled pe-none"}>
                            <Card id="popoverContainer">
                                <CardBody className="vstack gap-3">
                                    <div className="gap-1">
                                        <Label className="mb-0 md-label">
                                            Server Environment <Icon icon="info" color="info" id="EnvironmentInfo" />
                                            <UncontrolledPopover
                                                target="EnvironmentInfo"
                                                placement="right"
                                                trigger="hover"
                                                container="popoverContainer"
                                            >
                                                <PopoverBody>
                                                    <ul>
                                                        <li className="margin-bottom-xs">
                                                            Apply a <strong>tag</strong> to the Studio indicating the
                                                            server environment.
                                                        </li>
                                                        <li>This does not affect any settings or features.</li>
                                                    </ul>
                                                </PopoverBody>
                                            </UncontrolledPopover>
                                        </Label>
                                        <FormSelect
                                            control={control}
                                            name="environment"
                                            options={studioEnvironmentOptions}
                                            isSearchable={false}
                                        ></FormSelect>
                                    </div>
                                    <div className="gap-1">
                                        <Label className="mb-0 md-label">
                                            Default Replication Factor{" "}
                                            <Icon icon="info" color="info" id="ReplicationFactorInfo" />
                                            <UncontrolledPopover
                                                target="ReplicationFactorInfo"
                                                placement="right"
                                                trigger="hover"
                                                container="popoverContainer"
                                            >
                                                <PopoverBody>
                                                    <ul>
                                                        <li className="margin-bottom-xs">
                                                            Set the default <strong>replication factor</strong> when
                                                            creating a new database.
                                                        </li>
                                                        <li className="margin-bottom-xs">
                                                            {" "}
                                                            If not set, then the number of nodes in your cluster will be
                                                            used.
                                                        </li>
                                                        <li>
                                                            Additional nodes can always be added to the database after
                                                            it is created.
                                                        </li>
                                                    </ul>
                                                </PopoverBody>
                                            </UncontrolledPopover>
                                        </Label>
                                        <FormInput
                                            control={control}
                                            name="replicationFactor"
                                            type="number"
                                            placeholder="Cluster size (default)"
                                        ></FormInput>
                                    </div>
                                </CardBody>
                            </Card>
                            <Card className="mt-3">
                                <CardBody>
                                    <div className="d-flex flex-column">
                                        <FormSwitch control={control} name="isCollapseDocsWhenOpening">
                                            Collapse documents when opening
                                        </FormSwitch>
                                        <FormSwitch control={control} name="isSendUsageStats" className="mt-2">
                                            Help improve the Studio by gathering anonymous usage statistics
                                        </FormSwitch>
                                    </div>
                                </CardBody>
                            </Card>
                        </div>
                    </Form>
                </Col>
                <Col sm={12} md={4}>
                    <AboutViewAnchored defaultOpen={hasStudioConfiguration ? null : "licensing"}>
                        <AccordionItemWrapper
                            icon="about"
                            color="info"
                            heading="About this view"
                            description="Get additional info on this feature"
                            targetId="1"
                        >
                            <ul>
                                <li className="margin-bottom-xs">
                                    This is the <strong>Server-wide Studio-Configuration</strong> view.
                                    <br />
                                    The available studio-configuration options will apply server-wide to all databases.
                                </li>
                                <li>
                                    The environment tag can be customized per database in the Database
                                    Studio-Configuration view.
                                </li>
                            </ul>
                            <hr />
                            <div className="small-label mb-2">useful links</div>
                            <a href={clientConfigurationDocsLink} target="_blank">
                                <Icon icon="newtab" /> Docs - Client Configuration
                            </a>
                        </AccordionItemWrapper>
                        <FeatureAvailabilitySummaryWrapper
                            isUnlimited={hasStudioConfiguration}
                            data={featureAvailability}
                        />
                    </AboutViewAnchored>
                </Col>
            </Row>
        </div>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Studio Configuration",
        featureIcon: "studio-configuration",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
