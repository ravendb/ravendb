import { useEffect } from "react";
import Spinner from "react-bootstrap/Spinner";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormGroup, FormInput, FormRadioToggleWithIcon, FormSelect } from "components/common/Form";
import OverridableField from "components/common/OverridableField";
import FieldLabel from "components/common/FieldLabel";
import {
    IdentityPartsSeparatorTooltip,
    LoadBalanceBehaviorTooltip,
    LoadBalancerSeedTooltip,
    MaximumNumberOfRequestsTooltip,
    ReadBalanceBehaviorTooltip,
} from "components/common/clientConfiguration/ClientConfigurationTooltips";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import {
    ClientConfigurationFormData,
    clientConfigurationYupResolver,
} from "components/common/clientConfiguration/ClientConfigurationValidation";
import { Icon } from "components/common/Icon";
import appUrl = require("common/appUrl");
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";
import useClientConfigurationFormSideEffects from "components/common/clientConfiguration/useClientConfigurationFormSideEffects";
import { tryHandleSubmit } from "components/utils/common";
import classNames from "classnames";
import { RadioToggleWithIconInputItem } from "components/common/toggles/RadioToggle";
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
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { ConditionalPopover } from "components/common/ConditionalPopover";

export default function ClientDatabaseConfiguration() {
    const { manageServerService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const asyncGetClientGlobalConfiguration = useAsyncCallback(async () => {
        const globalConfigResult = await manageServerService.getGlobalClientConfiguration();
        if (!globalConfigResult) {
            return null;
        }

        return ClientConfigurationUtils.mapToFormData(globalConfigResult);
    });

    const asyncGetDefaultValues = useAsyncCallback(async () => {
        try {
            const globalConfiguration = await asyncGetClientGlobalConfiguration.execute();
            const clientConfiguration = await manageServerService.getClientConfiguration(databaseName);

            const overrideConfig = !globalConfiguration || (clientConfiguration && !clientConfiguration.Disabled);

            return ClientConfigurationUtils.mapToFormData(clientConfiguration, overrideConfig);
        } catch {
            return ClientConfigurationUtils.mapToFormData(null);
        }
    });

    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    const { handleSubmit, control, formState, watch, reset, setValue } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "all",
        defaultValues: asyncGetDefaultValues.execute,
    });

    useDirtyFlag(formState.isDirty);

    const loadBalancingLink = useRavenLink({ hash: "GYJ8JA" });
    const clientConfigurationLink = useRavenLink({ hash: "XYJ3B3" });

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

    const formValues = useWatch({ control: control });

    useClientConfigurationFormSideEffects(watch, setValue);

    useEffect(() => {
        if (formState.isSubmitSuccessful) {
            reset(formValues);
        }
    }, [formState.isSubmitSuccessful, reset, formValues]);

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData) => {
        tryHandleSubmit(async () => {
            await manageServerService.saveClientConfiguration(
                ClientConfigurationUtils.mapToDto(formData, false),
                databaseName
            );
        });
    };

    const onRefresh = async () => {
        reset(await asyncGetDefaultValues.execute());
    };

    const globalConfig = asyncGetClientGlobalConfiguration.result;

    if (asyncGetDefaultValues.loading) {
        return <LoadingView />;
    }

    if (asyncGetDefaultValues.error) {
        return <LoadError error="Unable to load client configuration" refresh={onRefresh} />;
    }

    const canEditDatabaseConfig = formValues.overrideConfig || !globalConfig;

    return (
        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
            <div className="content-margin">
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading
                            icon="database-client-configuration"
                            title="Client Configuration"
                            licenseBadgeText={hasClientConfiguration ? null : "Professional +"}
                        />
                        <div className="d-flex align-items-center justify-content-between flex-wrap gap-3 mb-3">
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
                                >
                                    {formState.isSubmitting ? (
                                        <Spinner size="sm" className="me-1" />
                                    ) : (
                                        <i className="icon-save me-1" />
                                    )}
                                    Save
                                </Button>
                            </ConditionalPopover>

                            {isClusterAdminOrClusterNode && (
                                <small title="Navigate to the server-wide Client Configuration View">
                                    <a target="_blank" href={appUrl.forGlobalClientConfiguration()}>
                                        <Icon icon="link" />
                                        Go to Server-Wide Client Configuration View
                                    </a>
                                </small>
                            )}
                        </div>
                        <div className={hasClientConfiguration ? "" : "item-disabled pe-none"}>
                            {globalConfig && (
                                <div className="mt-4 mb-3">
                                    <div className="w-fit-content mx-auto">
                                        <FormRadioToggleWithIcon
                                            name="overrideConfig"
                                            control={control}
                                            leftItem={leftRadioToggleItem}
                                            rightItem={rightRadioToggleItem}
                                        />
                                    </div>
                                </div>
                            )}

                            <Row>
                                {globalConfig && (
                                    <Col className="d-flex flex-column">
                                        <h4 className="mb-3">
                                            <Icon icon="server" />
                                            Server Configuration
                                            {isClusterAdminOrClusterNode && (
                                                <a
                                                    target="_blank"
                                                    href={appUrl.forGlobalClientConfiguration()}
                                                    className="ms-1 no-decor"
                                                    title="Server settings"
                                                >
                                                    <Icon icon="link" />
                                                </a>
                                            )}
                                        </h4>
                                        <Card
                                            className={classNames("flex-grow-1", {
                                                "item-disabled": canEditDatabaseConfig,
                                            })}
                                        >
                                            <div className="p-4 vstack gap-3">
                                                <FormGroup marginClass="">
                                                    <FieldLabel
                                                        tooltip={<IdentityPartsSeparatorTooltip />}
                                                        tooltipPlacement="right"
                                                    >
                                                        Identity parts separator
                                                    </FieldLabel>
                                                    <Form.Control
                                                        defaultValue={globalConfig.identityPartsSeparatorValue}
                                                        disabled
                                                        placeholder={
                                                            globalConfig.identityPartsSeparatorValue || "Default ('/')"
                                                        }
                                                    />
                                                </FormGroup>
                                                <FormGroup marginClass="">
                                                    <FieldLabel
                                                        tooltip={<MaximumNumberOfRequestsTooltip />}
                                                        tooltipPlacement="right"
                                                    >
                                                        Maximum number of requests per session
                                                    </FieldLabel>
                                                    <Form.Control
                                                        defaultValue={globalConfig.maximumNumberOfRequestsValue}
                                                        disabled
                                                        placeholder={
                                                            globalConfig.maximumNumberOfRequestsValue
                                                                ? globalConfig.maximumNumberOfRequestsValue.toLocaleString()
                                                                : "Default (30)"
                                                        }
                                                    />
                                                </FormGroup>
                                            </div>
                                        </Card>
                                    </Col>
                                )}
                                <Col className="d-flex flex-column">
                                    <h4 className="mb-3">
                                        <Icon icon="database" />
                                        Database Configuration
                                    </h4>
                                    <Card
                                        className={classNames("flex-grow-1", {
                                            "item-disabled": !canEditDatabaseConfig,
                                        })}
                                    >
                                        <div className="p-4 vstack gap-3">
                                            <OverridableField
                                                control={control}
                                                overrideName="identityPartsSeparatorEnabled"
                                                tooltipPlacement="right"
                                                label="Identity parts separator"
                                                tooltip={<IdentityPartsSeparatorTooltip />}
                                                disabled={!canEditDatabaseConfig}
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
                                                disabled={!canEditDatabaseConfig}
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
                                        </div>
                                    </Card>
                                </Col>
                            </Row>

                            <div
                                className={classNames(
                                    "d-flex mt-3 position-relative",
                                    { "justify-content-center": globalConfig },
                                    { "justify-content-between": !globalConfig }
                                )}
                            >
                                <h5 className={globalConfig && "text-center"}>Load Balancing Client Requests</h5>
                                <small title="Navigate to the documentation" className="position-absolute end-0">
                                    <a href={loadBalancingLink} target="_blank">
                                        <Icon icon="link" /> Load balancing tutorial
                                    </a>
                                </small>
                            </div>

                            <Row>
                                {globalConfig && (
                                    <Col className="d-flex flex-column">
                                        <Card
                                            className={classNames("p-4 vstack gap-3", "flex-grow-1", {
                                                "item-disabled": canEditDatabaseConfig,
                                            })}
                                        >
                                            <FormGroup marginClass="">
                                                <FieldLabel
                                                    tooltip={<LoadBalanceBehaviorTooltip />}
                                                    tooltipPlacement="right"
                                                >
                                                    Load Balance Behavior
                                                </FieldLabel>
                                                <Form.Control
                                                    defaultValue={globalConfig.loadBalancerValue}
                                                    disabled
                                                    placeholder="None"
                                                />
                                            </FormGroup>
                                            {(globalConfig?.loadBalancerSeedValue ||
                                                formValues.loadBalancerValue === "UseSessionContext") && (
                                                <FormGroup marginClass="">
                                                    <FieldLabel
                                                        tooltip={<LoadBalancerSeedTooltip />}
                                                        tooltipPlacement="right"
                                                    >
                                                        Seed
                                                    </FieldLabel>
                                                    <Form.Control
                                                        defaultValue={globalConfig.loadBalancerSeedValue}
                                                        disabled
                                                        placeholder="Default (0)"
                                                    />
                                                </FormGroup>
                                            )}
                                            <FormGroup marginClass="">
                                                <FieldLabel
                                                    tooltip={<ReadBalanceBehaviorTooltip />}
                                                    tooltipPlacement="right"
                                                >
                                                    Read Balance Behavior
                                                </FieldLabel>
                                                <Form.Control
                                                    defaultValue={globalConfig.readBalanceBehaviorValue}
                                                    placeholder="None"
                                                    disabled
                                                />
                                            </FormGroup>
                                        </Card>
                                    </Col>
                                )}
                                <Col className="d-flex flex-column">
                                    <Card
                                        className={classNames("p-4 vstack gap-3", "flex-grow-1", {
                                            "item-disabled": !canEditDatabaseConfig,
                                        })}
                                    >
                                        <OverridableField
                                            control={control}
                                            overrideName="loadBalancerEnabled"
                                            tooltipPlacement="right"
                                            label="Load Balance Behavior"
                                            tooltip={<LoadBalanceBehaviorTooltip />}
                                            disabled={!canEditDatabaseConfig}
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
                                        {(globalConfig?.loadBalancerSeedValue ||
                                            formValues.loadBalancerValue === "UseSessionContext") && (
                                            <OverridableField
                                                control={control}
                                                overrideName="loadBalancerSeedEnabled"
                                                tooltipPlacement="right"
                                                label="Seed"
                                                tooltip={<LoadBalancerSeedTooltip />}
                                                disabled={
                                                    formValues.loadBalancerValue !== "UseSessionContext" ||
                                                    !canEditDatabaseConfig
                                                }
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
                                            disabled={!canEditDatabaseConfig}
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
                                </Col>
                            </Row>
                        </div>
                    </Col>
                    <Col sm={12} md={4}>
                        <AboutViewAnchored defaultOpen={hasClientConfiguration ? null : "licensing"}>
                            <AccordionItemWrapper icon="about" color="info" targetId="1">
                                <ul>
                                    <li className="margin-bottom-xs">
                                        This is the <strong>Database Client-Configuration</strong> view.
                                        <br />
                                        The values set in this view will apply to any client communicating with this
                                        database.
                                    </li>
                                    <li>
                                        If the <strong>Server-wide Client-Configuration</strong> view has any values
                                        set,
                                        <br /> then this view provides the option to override the Server-wide
                                        Client-Configuration
                                        <br /> and customize specific values for this database.
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
                                <a href={clientConfigurationLink} target="_blank">
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
            <div id="PopoverContainer"></div>
        </Form>
    );
}

const leftRadioToggleItem: RadioToggleWithIconInputItem<boolean> = {
    label: "Use server config",
    value: false,
    iconName: "server",
};

const rightRadioToggleItem: RadioToggleWithIconInputItem<boolean> = {
    label: "Use database config",
    value: true,
    iconName: "database",
};

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
