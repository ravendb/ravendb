import React, { useEffect, useMemo } from "react";
import { Form, Col, Button, Row, Spinner, Input, InputGroupText, InputGroup, UncontrolledPopover } from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormCheckbox, FormInput, FormRadioToggleWithIcon, FormSelect, FormSwitch } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { useAccessManager } from "hooks/useAccessManager";
import { LoadError } from "components/common/LoadError";
import database = require("models/resources/database");
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
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import {
    AboutViewAnchored,
    AboutViewHeading,
    AccordionItemLicensing,
    AccordionItemWrapper,
} from "components/common/AboutView";
import Code from "components/common/Code";

interface ClientDatabaseConfigurationProps {
    db: database;
    licenseType?: string;
}

export default function ClientDatabaseConfiguration({ db, licenseType }: ClientDatabaseConfigurationProps) {
    const { manageServerService } = useServices();
    const asyncGetClientConfiguration = useAsyncCallback(manageServerService.getClientConfiguration);
    const asyncGetClientGlobalConfiguration = useAsync(manageServerService.getGlobalClientConfiguration, []);

    const { isClusterAdminOrClusterNode: canNavigateToServerSettings } = useAccessManager();

    const { handleSubmit, control, formState, setValue, reset } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "all",
        defaultValues: async () =>
            ClientConfigurationUtils.mapToFormData(await asyncGetClientConfiguration.execute(db), false),
    });

    useDirtyFlag(formState.isDirty);

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
            await manageServerService.saveClientConfiguration(ClientConfigurationUtils.mapToDto(formData, false), db);
        });
    };

    const onRefresh = async () => {
        reset(ClientConfigurationUtils.mapToFormData(await asyncGetClientConfiguration.execute(db), false));
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
                            badge={licenseType === "community"}
                            badgeText={licenseType === "community" ? "Professional +" : undefined}
                        />
                        <div className="d-flex align-items-center justify-content-between flex-wrap gap-3 mb-3">
                            <div>
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

                            {canNavigateToServerSettings() && (
                                <small title="Navigate to the server-wide Client Configuration View">
                                    <a target="_blank" href={appUrl.forGlobalClientConfiguration()}>
                                        <Icon icon="link" />
                                        Go to Server-Wide Client Configuration View
                                    </a>
                                </small>
                            )}
                        </div>
                        <div className={licenseType === "community" ? "item-disabled pe-none" : ""}>
                            {globalConfig && (
                                <div className="mt-4 mb-3">
                                    <div className="hstack justify-content-center">
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
                                        <RichPanel className={canEditDatabaseConfig && "item-disabled"}>
                                            <RichPanelHeader className="px-4 py-2 gap-2">
                                                <h3 className="mb-0">
                                                    <Icon icon="server" />
                                                    Server Configuration
                                                </h3>
                                                {canNavigateToServerSettings() && (
                                                    <a
                                                        target="_blank"
                                                        href={appUrl.forGlobalClientConfiguration()}
                                                        className="me-1 no-decor"
                                                        title="Server settings"
                                                    >
                                                        <Icon icon="link" />
                                                    </a>
                                                )}
                                            </RichPanelHeader>
                                            <div className="p-4">
                                                <div className="md-label">
                                                    Identity parts separator{" "}
                                                    <Icon id="SetIdentityPartsSeparator" icon="info" color="info" />
                                                </div>
                                                <UncontrolledPopover
                                                    target="SetIdentityPartsSeparator"
                                                    trigger="hover"
                                                    container="PopoverContainer"
                                                    placement="top"
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
                                                    <Icon id="SetMaximumRequestsPerSession" icon="info" color="info" />
                                                </div>
                                                <UncontrolledPopover
                                                    target="SetMaximumRequestsPerSession"
                                                    trigger="hover"
                                                    container="PopoverContainer"
                                                    placement="top"
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
                                        </RichPanel>
                                    </Col>
                                )}
                                <Col>
                                    <RichPanel className={!canEditDatabaseConfig && "item-disabled"}>
                                        <RichPanelHeader className="px-4 py-2">
                                            <h3 className="mb-0">
                                                <Icon icon="database" />
                                                Database Configuration
                                            </h3>
                                        </RichPanelHeader>
                                        <div className="p-4">
                                            <div className="md-label">
                                                Identity parts separator{" "}
                                                <Icon id="SetIdentityPartsSeparator" icon="info" color="info" />
                                            </div>
                                            <UncontrolledPopover
                                                target="SetIdentityPartsSeparator"
                                                trigger="hover"
                                                container="PopoverContainer"
                                                placement="top"
                                            >
                                                <div className="p-3">
                                                    Set the default separator for automatically generated document
                                                    identity IDs.
                                                    <br />
                                                    Use any character except <code>&apos;|&apos;</code> (pipe).
                                                </div>
                                            </UncontrolledPopover>
                                            <InputGroup>
                                                <InputGroupText>
                                                    <FormCheckbox
                                                        control={control}
                                                        name="identityPartsSeparatorEnabled"
                                                        disabled={!canEditDatabaseConfig}
                                                        color="primary"
                                                    />
                                                </InputGroupText>
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
                                                <Icon id="SetMaximumRequestsPerSession" icon="info" color="info" />
                                            </div>
                                            <UncontrolledPopover
                                                target="SetMaximumRequestsPerSession"
                                                trigger="hover"
                                                container="PopoverContainer"
                                                placement="top"
                                            >
                                                <div className="p-3">
                                                    Set this number to restrict the number of requests (
                                                    <code>Reads</code> & <code>Writes</code>) per session in the client
                                                    API.
                                                </div>
                                            </UncontrolledPopover>
                                            <InputGroup>
                                                <InputGroupText>
                                                    <FormCheckbox
                                                        control={control}
                                                        name="maximumNumberOfRequestsEnabled"
                                                        disabled={!canEditDatabaseConfig}
                                                        color="primary"
                                                    />
                                                </InputGroupText>
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
                                    </RichPanel>
                                </Col>
                            </Row>

                            <div
                                className={classNames(
                                    "d-flex mt-4 position-relative",
                                    { "justify-content-center": globalConfig },
                                    { "justify-content-between": !globalConfig }
                                )}
                            >
                                <h4 className={globalConfig && "text-center"}>Load Balancing Client Requests</h4>
                                <small title="Navigate to the documentation" className="position-absolute end-0">
                                    <a href="https://ravendb.net/l/GYJ8JA/latest/csharp" target="_blank">
                                        <Icon icon="link" /> Load balancing tutorial
                                    </a>
                                </small>
                            </div>

                            <Row>
                                {globalConfig && (
                                    <Col>
                                        <RichPanel
                                            className={classNames("p-4", { "item-disabled": canEditDatabaseConfig })}
                                        >
                                            <div className="md-label">
                                                Load Balance Behavior{" "}
                                                <Icon id="SetSessionContext" icon="info" color="info" />
                                            </div>
                                            <UncontrolledPopover
                                                target="SetSessionContext"
                                                trigger="hover"
                                                container="PopoverContainer"
                                                placement="top"
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
                                                            Read requests - the node the client will target will be
                                                            based the Read balance behavior configuration.
                                                            <br />
                                                            Write requests - will be sent to the preferred node.
                                                        </li>
                                                        <li className="mb-1">
                                                            <code>Use session context</code>
                                                            <br />
                                                            Sessions that are assigned the same context will have all
                                                            their Read & Write requests routed to the same node.
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
                                                            id="SetLoadBalanceSeedBehavior"
                                                            icon="info"
                                                            color="info"
                                                        />
                                                        <UncontrolledPopover
                                                            target="SetLoadBalanceSeedBehavior"
                                                            trigger="hover"
                                                            container="PopoverContainer"
                                                            placement="top"
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
                                                <Icon id="SetReadBalanceBehavior" icon="info" color="info" />
                                                <UncontrolledPopover
                                                    target="SetReadBalanceBehavior"
                                                    trigger="hover"
                                                    container="PopoverContainer"
                                                    placement="top"
                                                >
                                                    <div className="p-3">
                                                        <div>
                                                            Set the Read balance method the client will use when
                                                            accessing a node with
                                                            <code> Read</code> requests.
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
                                        </RichPanel>
                                    </Col>
                                )}
                                <Col>
                                    <RichPanel
                                        className={classNames("p-4", { "item-disabled": !canEditDatabaseConfig })}
                                    >
                                        <div className="md-label">
                                            Load Balance Behavior{" "}
                                            <Icon id="SetSessionContext" icon="info" color="info" />
                                        </div>
                                        <UncontrolledPopover
                                            target="SetSessionContext"
                                            trigger="hover"
                                            container="PopoverContainer"
                                            placement="top"
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
                                                        Read requests - the node the client will target will be based
                                                        the Read balance behavior configuration.
                                                        <br />
                                                        Write requests - will be sent to the preferred node.
                                                    </li>
                                                    <li className="mb-1">
                                                        <code>Use session context</code>
                                                        <br />
                                                        Sessions that are assigned the same context will have all their
                                                        Read & Write requests routed to the same node.
                                                        <br />
                                                        The session context is hashed from a context string (given by
                                                        the client) and an optional seed.
                                                    </li>
                                                </ul>
                                            </div>
                                        </UncontrolledPopover>
                                        <InputGroup>
                                            <InputGroupText>
                                                <FormCheckbox
                                                    control={control}
                                                    name="loadBalancerEnabled"
                                                    disabled={!canEditDatabaseConfig}
                                                    color="primary"
                                                />
                                            </InputGroupText>
                                            <FormSelect
                                                control={control}
                                                name="loadBalancerValue"
                                                disabled={!formValues.loadBalancerEnabled || !canEditDatabaseConfig}
                                                options={ClientConfigurationUtils.getLoadBalanceBehaviorOptions()}
                                            />
                                        </InputGroup>
                                        {(globalConfig?.loadBalancerSeedValue ||
                                            formValues.loadBalancerValue === "UseSessionContext") && (
                                            <>
                                                <div className="md-label mt-4">
                                                    {" "}
                                                    Seed
                                                    <Icon id="SetLoadBalanceSeedBehavior" icon="info" color="info" />
                                                    <UncontrolledPopover
                                                        target="SetLoadBalanceSeedBehavior"
                                                        trigger="hover"
                                                        container="PopoverContainer"
                                                        placement="top"
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
                                            <Icon id="SetReadBalanceBehavior" icon="info" color="info" />
                                            <UncontrolledPopover
                                                target="SetReadBalanceBehavior"
                                                trigger="hover"
                                                container="PopoverContainer"
                                                placement="top"
                                            >
                                                <div className="p-3">
                                                    <div>
                                                        Set the Read balance method the client will use when accessing a
                                                        node with
                                                        <code> Read</code> requests.
                                                        <br />
                                                        <code>Write</code> requests are sent to the preferred node.
                                                    </div>
                                                </div>
                                            </UncontrolledPopover>
                                        </div>
                                        <InputGroup>
                                            <InputGroupText>
                                                <FormCheckbox
                                                    control={control}
                                                    name="readBalanceBehaviorEnabled"
                                                    disabled={!canEditDatabaseConfig}
                                                    color="primary"
                                                />
                                            </InputGroupText>
                                            <FormSelect
                                                control={control}
                                                name="readBalanceBehaviorValue"
                                                disabled={
                                                    !formValues.readBalanceBehaviorEnabled || !canEditDatabaseConfig
                                                }
                                                options={ClientConfigurationUtils.getReadBalanceBehaviorOptions()}
                                            />
                                        </InputGroup>
                                    </RichPanel>
                                </Col>
                            </Row>
                        </div>
                    </Col>
                    <Col sm={12} md={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper
                                icon="about"
                                color="info"
                                heading="About this view"
                                description="Get additional info on what this feature can offer you"
                                targetId="1"
                            >
                                <p>
                                    <strong>Client Configuration</strong> lorem ipsum
                                </p>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href="https://ravendb.net/l/TS7SGF/6.0/Csharp" target="_blank">
                                    <Icon icon="newtab" /> Docs - Client Configuration
                                </a>
                            </AccordionItemWrapper>
                            {licenseType === "community" && (
                                <AccordionItemWrapper
                                    icon="license"
                                    color="warning"
                                    heading="Licensing"
                                    description="See which plans offer this and more exciting features"
                                    targetId="licensing"
                                    pill
                                    pillText="Upgrade available"
                                    pillIcon="star-filled"
                                >
                                    <AccordionItemLicensing
                                        description="This feature is not available in your license. Unleash the full potential and upgrade your plan."
                                        featureName="Client Configuration"
                                        featureIcon="database-client-configuration"
                                        checkedLicenses={["Professional", "Enterprise"]}
                                    >
                                        <p className="lead fs-4">Get your license expanded</p>
                                        <div className="mb-3">
                                            <Button color="primary" className="rounded-pill">
                                                <Icon icon="notifications" />
                                                Contact us
                                            </Button>
                                        </div>
                                        <small>
                                            <a href="https://ravendb.net/buy" target="_blank" className="text-muted">
                                                See pricing plans
                                            </a>
                                        </small>
                                    </AccordionItemLicensing>
                                </AccordionItemWrapper>
                            )}
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
