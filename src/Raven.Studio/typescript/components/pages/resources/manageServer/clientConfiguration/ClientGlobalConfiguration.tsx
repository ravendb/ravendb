import { useEffect } from "react";
import Spinner from "react-bootstrap/Spinner";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormInput, FormSelect } from "components/common/Form";
import OverridableField from "components/common/OverridableField";
import {
    IdentityPartsSeparatorTooltip,
    LoadBalanceBehaviorTooltip,
    LoadBalancerSeedTooltip,
    MaximumNumberOfRequestsTooltip,
    ReadBalanceBehaviorTooltip,
} from "components/common/clientConfiguration/ClientConfigurationTooltips";
import {
    ClientConfigurationFormData,
    clientConfigurationYupResolver,
} from "../../../../common/clientConfiguration/ClientConfigurationValidation";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";
import useClientConfigurationFormSideEffects from "components/common/clientConfiguration/useClientConfigurationFormSideEffects";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import { ConditionalPopover } from "components/common/ConditionalPopover";

export default function ClientGlobalConfiguration() {
    const { manageServerService } = useServices();
    const asyncGetGlobalClientConfiguration = useAsyncCallback(manageServerService.getGlobalClientConfiguration);

    const { handleSubmit, control, formState, setValue, reset, watch } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "all",
        defaultValues: async () =>
            ClientConfigurationUtils.mapToFormData(await asyncGetGlobalClientConfiguration.execute()),
    });

    const loadBalancingDocsLink = useRavenLink({ hash: "GYJ8JA" });
    const clientConfigurationDocsLink = useRavenLink({ hash: "TS7SGF" });

    const formValues = useWatch({ control });

    useClientConfigurationFormSideEffects(watch, setValue);

    useEffect(() => {
        if (formState.isSubmitSuccessful) {
            reset(formValues);
        }
    }, [formState.isSubmitSuccessful, reset, formValues]);

    useDirtyFlag(formState.isDirty);

    const hasClientConfiguration = useAppSelector(licenseSelectors.statusValue("HasClientConfiguration"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasClientConfiguration,
            },
        ],
    });

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveGlobalClientConfiguration(ClientConfigurationUtils.mapToDto(formData, true));
        });
    };

    const onRefresh = async () => {
        reset(ClientConfigurationUtils.mapToFormData(await asyncGetGlobalClientConfiguration.execute()));
    };

    if (asyncGetGlobalClientConfiguration.loading) {
        return <LoadingView />;
    }

    if (asyncGetGlobalClientConfiguration.error) {
        return <LoadError error="Unable to load client global configuration" refresh={onRefresh} />;
    }

    return (
        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
            <div className="content-margin">
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading
                            icon="database-client-configuration"
                            title="Server-Wide Client Configuration"
                            licenseBadgeText={hasClientConfiguration ? null : "Professional +"}
                        />
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasClientConfiguration,
                                message: <FeatureNotAvailableInYourLicensePopoverBody />,
                            }}
                        >
                            <Button
                                type="submit"
                                variant="primary"
                                disabled={formState.isSubmitting || !formState.isDirty}
                                className="mb-3"
                            >
                                {formState.isSubmitting ? <Spinner size="sm" className="me-1" /> : <Icon icon="save" />}
                                Save
                            </Button>
                        </ConditionalPopover>
                        <div className={hasClientConfiguration ? "" : "item-disabled pe-none"}>
                            <Card className="p-4 vstack gap-3">
                                <OverridableField
                                    control={control}
                                    overrideName="identityPartsSeparatorEnabled"
                                    tooltipPlacement="right"
                                    label="Identity parts separator"
                                    tooltip={<IdentityPartsSeparatorTooltip />}
                                >
                                    {({ isDisabled }) => (
                                        <FormInput
                                            type="text"
                                            control={control}
                                            name="identityPartsSeparatorValue"
                                            placeholder="Default ('/')"
                                            disabled={isDisabled}
                                        />
                                    )}
                                </OverridableField>
                                <OverridableField
                                    control={control}
                                    overrideName="maximumNumberOfRequestsEnabled"
                                    tooltipPlacement="right"
                                    label="Maximum number of requests per session"
                                    tooltip={<MaximumNumberOfRequestsTooltip />}
                                >
                                    {({ isDisabled }) => (
                                        <FormInput
                                            type="number"
                                            control={control}
                                            name="maximumNumberOfRequestsValue"
                                            placeholder="Default (30)"
                                            disabled={isDisabled}
                                        />
                                    )}
                                </OverridableField>
                            </Card>
                            <div className="d-flex justify-content-between mt-4 position-relative">
                                <h5>Load Balancing Client Requests</h5>
                                <small title="Navigate to the documentation" className="position-absolute end-0">
                                    <a href={loadBalancingDocsLink} target="_blank">
                                        <Icon icon="link" /> Load balancing tutorial
                                    </a>
                                </small>
                            </div>
                            <Card className="p-4 vstack gap-3">
                                <OverridableField
                                    control={control}
                                    overrideName="loadBalancerEnabled"
                                    tooltipPlacement="right"
                                    label="Load Balance Behavior"
                                    tooltip={<LoadBalanceBehaviorTooltip />}
                                >
                                    {({ isDisabled, controlId }) => (
                                        <FormSelect
                                            control={control}
                                            name="loadBalancerValue"
                                            isDisabled={isDisabled}
                                            inputId={controlId}
                                            options={ClientConfigurationUtils.getLoadBalanceBehaviorOptions()}
                                            isSearchable={false}
                                        />
                                    )}
                                </OverridableField>
                                {formValues.loadBalancerValue === "UseSessionContext" && (
                                    <OverridableField
                                        control={control}
                                        overrideName="loadBalancerSeedEnabled"
                                        tooltipPlacement="right"
                                        label="Seed"
                                        tooltip={<LoadBalancerSeedTooltip />}
                                    >
                                        {({ isDisabled }) => (
                                            <FormInput
                                                type="number"
                                                control={control}
                                                name="loadBalancerSeedValue"
                                                placeholder="Default (0)"
                                                disabled={isDisabled}
                                            />
                                        )}
                                    </OverridableField>
                                )}
                                <OverridableField
                                    control={control}
                                    overrideName="readBalanceBehaviorEnabled"
                                    tooltipPlacement="right"
                                    label="Read Balance Behavior"
                                    tooltip={<ReadBalanceBehaviorTooltip />}
                                >
                                    {({ isDisabled, controlId }) => (
                                        <FormSelect
                                            control={control}
                                            name="readBalanceBehaviorValue"
                                            isDisabled={isDisabled}
                                            inputId={controlId}
                                            options={ClientConfigurationUtils.getReadBalanceBehaviorOptions()}
                                            isSearchable={false}
                                        />
                                    )}
                                </OverridableField>
                            </Card>
                        </div>
                    </Col>
                    <Col sm={12} md={4}>
                        <AboutViewAnchored defaultOpen={hasClientConfiguration ? null : "licensing"}>
                            <AccordionItemWrapper icon="about" color="info" targetId="1">
                                <ul>
                                    <li className="margin-bottom-xs">
                                        This is the <strong>Server-wide Client-Configuration</strong> view.
                                        <br />
                                        The available Client-Configuration options will apply to any client that
                                        communicates with any database in the cluster.
                                    </li>
                                    <li>
                                        These values can be customized per database in the{" "}
                                        <strong>Database Client-Configuration</strong> view.
                                    </li>
                                </ul>
                                <hr />
                                <ul>
                                    <li className="margin-bottom-xs">
                                        Setting the Client-Configuration on the server from this view will{" "}
                                        <strong>override</strong> the client&apos;s existing settings, which were
                                        initially set by your client code.
                                    </li>
                                    <li className="margin-bottom-xs">
                                        When the server&apos;s Client-Configuration is modified, the running client will
                                        receive the updated settings the next time it makes a request to the server.
                                    </li>
                                    <li>
                                        This enables administrators to{" "}
                                        <strong>dynamically control the client behavior</strong> even after it has
                                        started running. E.g. manage load balancing of client requests on the fly in
                                        response to changing system demands.
                                    </li>
                                </ul>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href={clientConfigurationDocsLink} target="_blank">
                                    <Icon icon="newtab" /> Docs - Client Configuration
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={hasClientConfiguration}
                                data={featureAvailability}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </div>
        </Form>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Client Configuration",
        featureIcon: "client-configuration",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
        quill: { value: true },
    },
];
