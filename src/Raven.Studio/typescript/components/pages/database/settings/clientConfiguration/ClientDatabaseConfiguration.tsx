import React, { useEffect, useMemo } from "react";
import { Form, Col, Button, Row, Spinner, Input, InputGroup, UncontrolledPopover, Card } from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormCheckbox, FormInput, FormRadioToggleWithIcon, FormSelect, FormSwitch } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import {
    ClientConfigurationFormData,
    clientConfigurationYupResolver,
} from "components/common/clientConfiguration/ClientConfigurationValidation";
import { Icon } from "components/common/Icon";
import appUrl = require("common/appUrl");
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";
import useClientConfigurationFormController from "components/common/clientConfiguration/useClientConfigurationFormController";
import { tryHandleSubmit } from "components/utils/common";
import classNames from "classnames";
import { RadioToggleWithIconInputItem } from "components/common/RadioToggle";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import FeatureNotAvailableInYourLicensePopover from "components/common/FeatureNotAvailableInYourLicensePopover";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

export default function ClientDatabaseConfiguration() {
    const { manageServerService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const asyncGetClientConfiguration = useAsyncCallback(manageServerService.getClientConfiguration);
    const asyncGetClientGlobalConfiguration = useAsync(manageServerService.getGlobalClientConfiguration, []);

    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    const { handleSubmit, control, formState, setValue, reset } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "all",
        defaultValues: async () =>
            ClientConfigurationUtils.mapToFormData(await asyncGetClientConfiguration.execute(databaseName), false),
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

    const globalConfig = useMemo(() => {
        const globalConfigResult = asyncGetClientGlobalConfiguration.result;
        if (!globalConfigResult) {
            return null;
        }

        return ClientConfigurationUtils.mapToFormData(globalConfigResult, true);
    }, [asyncGetClientGlobalConfiguration.result]);

    const formValues = useClientConfigurationFormController(
        control,
        setValue,
        (asyncGetClientGlobalConfiguration.status === "success" && !globalConfig) ||
            asyncGetClientGlobalConfiguration.status === "error"
    );

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
        reset(ClientConfigurationUtils.mapToFormData(await asyncGetClientConfiguration.execute(databaseName), false));
    };

    if (asyncGetClientConfiguration.loading || asyncGetClientGlobalConfiguration.loading) {
        return <LoadingView />;
    }

    if (asyncGetClientConfiguration.error) {
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
                            <div id="saveClientConfiguration" className="w-fit-content">
                                <Button
                                    type="submit"
                                    color="primary"
                                    disabled={formState.isSubmitting || !formState.isDirty}
                                >
                                    {formState.isSubmitting ? (
                                        <Spinner size="sm" className="me-1" />
                                    ) : (
                                        <i className="icon-save me-1" />
                                    )}
                                    Save
                                </Button>
                            </div>
                            {!hasClientConfiguration && (
                                <FeatureNotAvailableInYourLicensePopover target="saveClientConfiguration" />
                            )}

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
                                    <Col>
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
                                        <Card className={canEditDatabaseConfig && "item-disabled"}>
                                            <div className="p-4">
                                                <div className="md-label">
                                                    Identity parts separator{" "}
                                                    <Icon
                                                        id="SetIdentityPartsSeparator-server"
                                                        icon="info"
                                                        color="info"
                                                    />
                                                </div>
                                                <UncontrolledPopover
                                                    target="SetIdentityPartsSeparator-server"
                                                    trigger="hover"
                                                    container="PopoverContainer"
                                                    placement="right"
                                                >
                                                    <div className="p-3">
                                                        Set the default separator for automatically generated document
                                                        identity IDs.
                                                        <br />
                                                        Use any character except <code>&apos;|&apos;</code> (pipe).
                                                    </div>
                                                </UncontrolledPopover>
                                                <Input
                                                    defaultValue={globalConfig.identityPartsSeparatorValue}
                                                    disabled
                                                    placeholder={
                                                        globalConfig.identityPartsSeparatorValue || "'/' (default)"
                                                    }
                                                />
                                                <div className="md-label mt-4">
                                                    Maximum number of requests per session{" "}
                                                    <Icon
                                                        id="SetMaximumRequestsPerSession-server"
                                                        icon="info"
                                                        color="info"
                                                    />
                                                </div>
                                                <UncontrolledPopover
                                                    target="SetMaximumRequestsPerSession-server"
                                                    trigger="hover"
                                                    container="PopoverContainer"
                                                    placement="right"
                                                >
                                                    <div className="p-3">
                                                        Set this number to restrict the number of requests (
                                                        <code>Reads</code> & <code>Writes</code>) per session in the
                                                        client API.
                                                    </div>
                                                </UncontrolledPopover>
                                                <Input
                                                    defaultValue={globalConfig.maximumNumberOfRequestsValue}
                                                    disabled
                                                    placeholder={
                                                        globalConfig.maximumNumberOfRequestsValue
                                                            ? globalConfig.maximumNumberOfRequestsValue.toLocaleString()
                                                            : "30 (default)"
                                                    }
                                                />
                                            </div>
                                        </Card>
                                    </Col>
                                )}
                                <Col>
                                    <h4 className="mb-3">
                                        <Icon icon="database" />
                                        Database Configuration
                                    </h4>
                                    <Card className={classNames({ "item-disabled": !canEditDatabaseConfig })}>
                                        <div className="p-4">
                                            <div className="md-label">
                                                Identity parts separator{" "}
                                                <Icon
                                                    id="SetIdentityPartsSeparator-database"
                                                    icon="info"
                                                    color="info"
                                                />
                                            </div>
                                            <UncontrolledPopover
                                                target="SetIdentityPartsSeparator-database"
                                                trigger="hover"
                                                container="PopoverContainer"
                                                placement="right"
                                            >
                                                <div className="p-3">
                                                    Set the default separator for automatically generated document
                                                    identity IDs.
                                                    <br />
                                                    Use any character except <code>&apos;|&apos;</code> (pipe).
                                                </div>
                                            </UncontrolledPopover>
                                            <InputGroup>
                                                <div className="toggle-field-checkbox">
                                                    <FormCheckbox
                                                        control={control}
                                                        name="identityPartsSeparatorEnabled"
                                                        disabled={!canEditDatabaseConfig}
                                                        color="primary"
                                                    />
                                                </div>
                                                <FormInput
                                                    type="text"
                                                    control={control}
                                                    name="identityPartsSeparatorValue"
                                                    placeholder="'/' (default)"
                                                    disabled={
                                                        !formValues.identityPartsSeparatorEnabled ||
                                                        !canEditDatabaseConfig
                                                    }
                                                    className="d-flex"
                                                />
                                            </InputGroup>
                                            <div className="md-label mt-4">
                                                Maximum number of requests per session{" "}
                                                <Icon
                                                    id="SetMaximumRequestsPerSession-database"
                                                    icon="info"
                                                    color="info"
                                                />
                                            </div>
                                            <UncontrolledPopover
                                                target="SetMaximumRequestsPerSession-database"
                                                trigger="hover"
                                                container="PopoverContainer"
                                                placement="right"
                                            >
                                                <div className="p-3">
                                                    Set this number to restrict the number of requests
                                                    <br />(<code>Reads</code> & <code>Writes</code>) per session in the
                                                    client API.
                                                </div>
                                            </UncontrolledPopover>
                                            <InputGroup>
                                                <div className="toggle-field-checkbox">
                                                    <FormCheckbox
                                                        control={control}
                                                        name="maximumNumberOfRequestsEnabled"
                                                        disabled={!canEditDatabaseConfig}
                                                        color="primary"
                                                    />
                                                </div>
                                                <FormInput
                                                    type="number"
                                                    control={control}
                                                    name="maximumNumberOfRequestsValue"
                                                    placeholder="30 (default)"
                                                    disabled={
                                                        !formValues.maximumNumberOfRequestsEnabled ||
                                                        !canEditDatabaseConfig
                                                    }
                                                />
                                            </InputGroup>
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
                                    <Col>
                                        <Card className={classNames("p-4", { "item-disabled": canEditDatabaseConfig })}>
                                            <div className="md-label">
                                                Load Balance Behavior{" "}
                                                <Icon id="SetSessionContext-server" icon="info" color="info" />
                                            </div>
                                            <UncontrolledPopover
                                                target="SetSessionContext-server"
                                                trigger="hover"
                                                container="PopoverContainer"
                                                placement="right"
                                            >
                                                <div className="p-3">
                                                    <span className="d-inline-block mb-1">
                                                        Set the Load balance method for <strong>Read</strong> &{" "}
                                                        <strong>Write</strong> requests.
                                                    </span>
                                                    <ul>
                                                        <li className="mb-1">
                                                            <code>None</code>
                                                            <br />
                                                            <strong>Read</strong> requests - the node the client will
                                                            target will be based on Read balance behavior configuration.
                                                            <br />
                                                            <strong>Write</strong> requests - will be sent to the
                                                            preferred node.
                                                        </li>
                                                        <li className="mb-1">
                                                            <code>Use session context</code>
                                                            <br />
                                                            Sessions that are assigned the same context will have all
                                                            their <strong>Read & Write</strong> requests routed to the
                                                            same node.
                                                            <br />
                                                            The session context is hashed from a context string (given
                                                            by the client) and an optional seed.
                                                        </li>
                                                    </ul>
                                                </div>
                                            </UncontrolledPopover>
                                            <Input
                                                defaultValue={globalConfig.loadBalancerValue}
                                                disabled
                                                placeholder="None"
                                            />
                                            {(globalConfig?.loadBalancerSeedValue ||
                                                formValues.loadBalancerValue === "UseSessionContext") && (
                                                <>
                                                    <div className="md-label mt-4">
                                                        {" "}
                                                        Seed
                                                        <Icon
                                                            id="SetLoadBalanceSeedBehavior-server"
                                                            icon="info"
                                                            color="info"
                                                        />
                                                        <UncontrolledPopover
                                                            target="SetLoadBalanceSeedBehavior-server"
                                                            trigger="hover"
                                                            container="PopoverContainer"
                                                            placement="right"
                                                        >
                                                            <div className="p-3">
                                                                An optional seed number.
                                                                <br />
                                                                Used when hashing the session context.
                                                            </div>
                                                        </UncontrolledPopover>
                                                    </div>
                                                    <Input
                                                        defaultValue={globalConfig.loadBalancerSeedValue}
                                                        disabled
                                                        placeholder="0 (default)"
                                                    />
                                                </>
                                            )}
                                            <div className="md-label mt-4">
                                                Read Balance Behavior{" "}
                                                <Icon id="SetReadBalanceBehavior-server" icon="info" color="info" />
                                                <UncontrolledPopover
                                                    target="SetReadBalanceBehavior-server"
                                                    trigger="hover"
                                                    container="PopoverContainer"
                                                    placement="right"
                                                >
                                                    <div className="p-3">
                                                        <div>
                                                            Set the Read balance method the client will use when
                                                            accessing a node with <code>Read</code> requests.
                                                            <br />
                                                            <code>Write</code> requests are sent to the preferred node.
                                                        </div>
                                                    </div>
                                                </UncontrolledPopover>
                                            </div>
                                            <Input
                                                defaultValue={globalConfig.readBalanceBehaviorValue}
                                                placeholder="None"
                                                disabled
                                            />
                                        </Card>
                                    </Col>
                                )}
                                <Col>
                                    <Card className={classNames("p-4", { "item-disabled": !canEditDatabaseConfig })}>
                                        <div className="md-label">
                                            Load Balance Behavior{" "}
                                            <Icon id="SetSessionContext-database" icon="info" color="info" />
                                        </div>
                                        <UncontrolledPopover
                                            target="SetSessionContext-database"
                                            trigger="hover"
                                            container="PopoverContainer"
                                            placement="right"
                                        >
                                            <div className="p-3">
                                                <span className="d-inline-block mb-1">
                                                    Set the Load balance method for <strong>Read</strong> &{" "}
                                                    <strong>Write</strong> requests.
                                                </span>
                                                <ul>
                                                    <li className="mb-1">
                                                        <code>None</code>
                                                        <br />
                                                        <strong>Read</strong> requests - the node the client will target
                                                        will be based on Read balance behavior configuration.
                                                        <br />
                                                        <strong>Write</strong> requests - will be sent to the preferred
                                                        node.
                                                    </li>
                                                    <li className="mb-1">
                                                        <code>Use session context</code>
                                                        <br />
                                                        Sessions that are assigned the same context will have all their{" "}
                                                        <strong>Read & Write</strong> requests routed to the same node.
                                                        <br />
                                                        The session context is hashed from a context string (given by
                                                        the client) and an optional seed.
                                                    </li>
                                                </ul>
                                            </div>
                                        </UncontrolledPopover>
                                        <InputGroup>
                                            <div className="toggle-field-checkbox">
                                                <FormCheckbox
                                                    control={control}
                                                    name="loadBalancerEnabled"
                                                    disabled={!canEditDatabaseConfig}
                                                    color="primary"
                                                />
                                            </div>
                                            <FormSelect
                                                control={control}
                                                name="loadBalancerValue"
                                                isDisabled={!formValues.loadBalancerEnabled || !canEditDatabaseConfig}
                                                options={ClientConfigurationUtils.getLoadBalanceBehaviorOptions()}
                                                isSearchable={false}
                                            />
                                        </InputGroup>
                                        {(globalConfig?.loadBalancerSeedValue ||
                                            formValues.loadBalancerValue === "UseSessionContext") && (
                                            <>
                                                <div className="md-label mt-4">
                                                    {" "}
                                                    Seed
                                                    <Icon
                                                        id="SetLoadBalanceSeedBehavior-database"
                                                        icon="info"
                                                        color="info"
                                                    />
                                                    <UncontrolledPopover
                                                        target="SetLoadBalanceSeedBehavior-database"
                                                        trigger="hover"
                                                        container="PopoverContainer"
                                                        placement="right"
                                                    >
                                                        <div className="p-3">
                                                            An optional seed number.
                                                            <br />
                                                            Used when hashing the session context.
                                                        </div>
                                                    </UncontrolledPopover>
                                                </div>

                                                <div className="hstack gap-3">
                                                    <FormSwitch
                                                        control={control}
                                                        name="loadBalancerSeedEnabled"
                                                        color="primary"
                                                        label="Seed"
                                                        disabled={
                                                            formValues.loadBalancerValue !== "UseSessionContext" ||
                                                            !canEditDatabaseConfig
                                                        }
                                                        className="small"
                                                    ></FormSwitch>
                                                    <InputGroup>
                                                        <FormInput
                                                            type="number"
                                                            control={control}
                                                            name="loadBalancerSeedValue"
                                                            placeholder="0 (default)"
                                                            disabled={
                                                                !formValues.loadBalancerSeedEnabled ||
                                                                !canEditDatabaseConfig
                                                            }
                                                        />
                                                    </InputGroup>
                                                </div>
                                            </>
                                        )}
                                        <div className="md-label mt-4">
                                            Read Balance Behavior{" "}
                                            <Icon id="SetReadBalanceBehavior-database" icon="info" color="info" />
                                            <UncontrolledPopover
                                                target="SetReadBalanceBehavior-database"
                                                trigger="hover"
                                                container="PopoverContainer"
                                                placement="right"
                                            >
                                                <div className="p-3">
                                                    <div>
                                                        Set the Read balance method the client will use when accessing a
                                                        node with <code> Read</code> requests.
                                                        <br />
                                                        <code>Write</code> requests are sent to the preferred node.
                                                    </div>
                                                </div>
                                            </UncontrolledPopover>
                                        </div>
                                        <InputGroup>
                                            <div className="toggle-field-checkbox">
                                                <FormCheckbox
                                                    control={control}
                                                    name="readBalanceBehaviorEnabled"
                                                    disabled={!canEditDatabaseConfig}
                                                    color="primary"
                                                />
                                            </div>
                                            <FormSelect
                                                control={control}
                                                name="readBalanceBehaviorValue"
                                                isDisabled={
                                                    !formValues.readBalanceBehaviorEnabled || !canEditDatabaseConfig
                                                }
                                                options={ClientConfigurationUtils.getReadBalanceBehaviorOptions()}
                                                isSearchable={false}
                                            />
                                        </InputGroup>
                                    </Card>
                                </Col>
                            </Row>
                        </div>
                    </Col>
                    <Col sm={12} md={4}>
                        <AboutViewAnchored defaultOpen={hasClientConfiguration ? null : "licensing"}>
                            <AccordionItemWrapper
                                icon="about"
                                color="info"
                                heading="About this view"
                                description="Get additional info on this feature"
                                targetId="1"
                            >
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
    },
];
