import React, { useMemo } from "react";
import { Form, Col, Button, Card, Row, Spinner, Input, InputGroupText, InputGroup } from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormCheckbox, FormInput, FormSelect, FormSwitch } from "components/common/Form";
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
import { PopoverWithHover } from "components/common/PopoverWithHover";
import useClientConfigurationPopovers from "components/common/clientConfiguration/useClientConfigurationPopovers";
import { PropSummary, PropSummaryItem, PropSummaryName, PropSummaryValue } from "components/common/PropSummary";

interface ClientDatabaseConfigurationProps {
    db: database;
}

// TODO: show modal on exit intent if is dirty
export default function ClientDatabaseConfiguration({ db }: ClientDatabaseConfigurationProps) {
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

    const popovers = useClientConfigurationPopovers();
    const formValues = useClientConfigurationFormController(control, setValue);

    const globalConfig = useMemo(() => {
        const globalConfigResult = asyncGetClientGlobalConfiguration.result;
        if (!globalConfigResult) {
            return null;
        }

        return ClientConfigurationUtils.mapToFormData(globalConfigResult, true);
    }, [asyncGetClientGlobalConfiguration.result]);

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData) => {
        tryHandleSubmit(async () => {
            await manageServerService.saveClientConfiguration(ClientConfigurationUtils.mapToDto(formData, false), db);
            reset(null, { keepValues: true });
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

    return (
        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
            <Col md="12" lg={globalConfig ? 9 : 6} className="content-margin">
                <div className="d-flex align-items-center justify-content-between flex-wrap gap-3 mb-3">
                    <div>
                        <Button type="submit" color="primary" disabled={formState.isSubmitting || !formState.isDirty}>
                            {formState.isSubmitting ? (
                                <Spinner size="sm" className="me-1" />
                            ) : (
                                <i className="icon-save me-1" />
                            )}
                            Save
                        </Button>

                        {globalConfig && (
                            <span ref={popovers.setEffectiveConfiguration} className="ms-3 cursor-pointer text-info">
                                <Icon icon="config" />
                                See effective configuration
                            </span>
                        )}
                        <PopoverWithHover target={popovers.effectiveConfiguration} placement="right">
                            <div className="flex-horizontal p-1">
                                <PropSummary>
                                    <PropSummaryItem className="mb-1">
                                        <strong>Effective configuration for dbname</strong>
                                    </PropSummaryItem>
                                    <PropSummaryItem className="border-0">
                                        <PropSummaryName>Identity parts separator</PropSummaryName>
                                        <PropSummaryValue color="info">
                                            {getIdentityPartsSeparatorEffectiveValue(formValues, globalConfig)}
                                        </PropSummaryValue>
                                    </PropSummaryItem>
                                    <PropSummaryItem>
                                        <PropSummaryName>Max number of requests per session</PropSummaryName>
                                        <PropSummaryValue color="info">
                                            {getMaximumNumberOfRequestsEffectiveValue(formValues, globalConfig)}
                                        </PropSummaryValue>
                                    </PropSummaryItem>
                                    <PropSummaryItem>
                                        <PropSummaryName>Load Balance Behavior</PropSummaryName>
                                        <PropSummaryValue color="info">
                                            {getLoadBalancerEffectiveValue(formValues, globalConfig)}
                                        </PropSummaryValue>
                                    </PropSummaryItem>
                                    <PropSummaryItem>
                                        <PropSummaryName>Seed</PropSummaryName>
                                        <PropSummaryValue color="info">
                                            {getLoadBalancerSeedEffectiveValue(formValues, globalConfig)}
                                        </PropSummaryValue>
                                    </PropSummaryItem>
                                    <PropSummaryItem>
                                        <PropSummaryName>Read Balance Behavior</PropSummaryName>
                                        <PropSummaryValue color="info">
                                            {getReadBalanceBehaviorEffectiveValue(formValues, globalConfig)}
                                        </PropSummaryValue>
                                    </PropSummaryItem>
                                </PropSummary>
                            </div>
                        </PopoverWithHover>
                    </div>

                    {canNavigateToServerSettings() && (
                        <small title="Navigate to the server-wide Client Configuration View">
                            <a target="_blank" href={appUrl.forGlobalClientConfiguration()}>
                                <Icon icon="link" className="me-1" />
                                Go to Server-Wide Client Configuration View
                            </a>
                        </small>
                    )}
                </div>

                {globalConfig && (
                    <Row className="flex-grow-1 mt-4 mb-3">
                        <FormSwitch control={control} name="overrideConfig" color="primary" className="mt-1 mb-3">
                            Override server configuration
                        </FormSwitch>
                        <Col>
                            <div className="flex-horizontal gap-1">
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
                            </div>
                        </Col>
                        <Col>
                            <h3 className="mb-0">
                                <Icon icon="database" />
                                Database Configuration
                            </h3>
                        </Col>
                    </Row>
                )}

                <Card className="card flex-column p-3 mb-3">
                    <div className={globalConfig ? "d-flex flex-grow-1 justify-content-center" : "d-flex flex-grow-1"}>
                        <div className="md-label">
                            Identity parts separator{" "}
                            <i ref={popovers.setIdentityPartsSeparator} className="icon-info text-info" />
                        </div>
                        <PopoverWithHover target={popovers.identityPartsSeparator} placement="top">
                            <div className="flex-horizontal p-3">
                                <div>
                                    Set the default separator for automatically generated document identity IDs.
                                    <br />
                                    Use any character except <code>&apos;|&apos;</code> (pipe).
                                </div>
                            </div>
                        </PopoverWithHover>
                    </div>
                    <Row className="flex-grow-1 align-items-start">
                        {globalConfig && (
                            <>
                                <Col className="d-flex">
                                    <Input
                                        defaultValue={globalConfig.identityPartsSeparatorValue}
                                        disabled
                                        placeholder={globalConfig.identityPartsSeparatorValue || "'/' (default)"}
                                    />
                                </Col>
                                {formValues.overrideConfig && formValues.identityPartsSeparatorEnabled && (
                                    <GlobalSettingsSeparator />
                                )}
                            </>
                        )}
                        <Col className="d-flex">
                            <InputGroup>
                                <InputGroupText>
                                    <FormCheckbox
                                        control={control}
                                        name="identityPartsSeparatorEnabled"
                                        disabled={!formValues.overrideConfig}
                                    />
                                </InputGroupText>
                                <FormInput
                                    type="text"
                                    control={control}
                                    name="identityPartsSeparatorValue"
                                    placeholder="'/' (default)"
                                    disabled={!formValues.identityPartsSeparatorEnabled || !formValues.overrideConfig}
                                    className="d-flex"
                                />
                            </InputGroup>
                        </Col>
                    </Row>
                </Card>

                <Card className="flex-column mb-3 p-3">
                    <div className={globalConfig ? "d-flex flex-grow-1 justify-content-center" : "d-flex flex-grow-1"}>
                        <div className="md-label">
                            Maximum number of requests per session{" "}
                            <i ref={popovers.setMaximumRequestsPerSession} className="icon-info text-info" />
                        </div>
                        <PopoverWithHover target={popovers.maximumRequestsPerSession} placement="top">
                            <div className="flex-horizontal p-3">
                                <div>
                                    Set this number to restrict the number of requests (<strong>Reads</strong> &{" "}
                                    <strong>Writes</strong>) per session in the client API.
                                </div>
                            </div>
                        </PopoverWithHover>
                    </div>
                    <Row className="flex-grow-1 align-items-start">
                        {globalConfig && (
                            <>
                                <Col className="d-flex">
                                    <Input
                                        defaultValue={globalConfig.maximumNumberOfRequestsValue}
                                        disabled
                                        placeholder={
                                            globalConfig.maximumNumberOfRequestsValue
                                                ? globalConfig.maximumNumberOfRequestsValue.toLocaleString()
                                                : "30 (default)"
                                        }
                                    />
                                </Col>
                                {formValues.overrideConfig && formValues.maximumNumberOfRequestsEnabled && (
                                    <GlobalSettingsSeparator />
                                )}
                            </>
                        )}
                        <Col className="d-flex">
                            <InputGroup>
                                <InputGroupText>
                                    <FormCheckbox
                                        control={control}
                                        name="maximumNumberOfRequestsEnabled"
                                        disabled={!formValues.overrideConfig}
                                    />
                                </InputGroupText>
                                <FormInput
                                    type="number"
                                    control={control}
                                    name="maximumNumberOfRequestsValue"
                                    placeholder="30 (default)"
                                    disabled={!formValues.maximumNumberOfRequestsEnabled || !formValues.overrideConfig}
                                />
                            </InputGroup>
                        </Col>
                    </Row>
                </Card>
                <div
                    className={
                        globalConfig
                            ? "d-flex justify-content-center mt-4 position-relative"
                            : "d-flex justify-content-between mt-4 position-relative"
                    }
                >
                    <h4 className={globalConfig ? "text-center" : null}>Load Balancing Client Requests</h4>
                    <small title="Navigate to the documentation" className="position-absolute end-0">
                        <a href="https://ravendb.net/l/GYJ8JA/latest/csharp" target="_blank">
                            <Icon icon="link" /> Load balancing tutorial
                        </a>
                    </small>
                </div>
                <Card className="flex-column p-3">
                    <div className={globalConfig ? "d-flex flex-grow-1 justify-content-center" : "d-flex flex-grow-1"}>
                        <div className="md-label">
                            Load Balance Behavior <i ref={popovers.setSessionContext} className="icon-info text-info" />
                            <PopoverWithHover target={popovers.sessionContext} placement="top">
                                <div className="flex-horizontal p-3">
                                    <div>
                                        <span className="d-inline-block mb-1">
                                            Set the Load balance method for <strong>Read</strong> &{" "}
                                            <strong>Write</strong> requests.
                                        </span>
                                        <ul>
                                            <li className="mb-1">
                                                <strong>None:</strong>
                                                <br />
                                                Read requests - the node the client will target will be based the Read
                                                balance behavior configuration.
                                                <br />
                                                Write requests - will be sent to the preferred node.
                                            </li>
                                            <li className="mb-1">
                                                <strong>Use session context:</strong>
                                                <br />
                                                Sessions that are assigned the same context will have all their Read &
                                                Write requests routed to the same node.
                                                <br />
                                                The session context is hashed from a context string (given by the
                                                client) and an optional seed.
                                            </li>
                                        </ul>
                                    </div>
                                </div>
                            </PopoverWithHover>
                        </div>
                    </div>
                    <Row className="mb-4 align-items-start">
                        {globalConfig && (
                            <>
                                <Col className="d-flex">
                                    <Input
                                        defaultValue={globalConfig.loadBalancerSeedValue}
                                        disabled
                                        placeholder="None"
                                    />
                                </Col>
                                {formValues.overrideConfig && formValues.loadBalancerEnabled && (
                                    <GlobalSettingsSeparator />
                                )}
                            </>
                        )}
                        <Col className="d-flex align-items-center gap-3">
                            <InputGroup>
                                <InputGroupText>
                                    <FormCheckbox
                                        control={control}
                                        name="loadBalancerEnabled"
                                        disabled={!formValues.overrideConfig}
                                    />
                                </InputGroupText>
                                <FormSelect
                                    control={control}
                                    name="loadBalancerValue"
                                    disabled={!formValues.loadBalancerEnabled || !formValues.overrideConfig}
                                    options={ClientConfigurationUtils.getLoadBalanceBehaviorOptions()}
                                />
                            </InputGroup>
                        </Col>
                    </Row>
                    {formValues.loadBalancerValue === "UseSessionContext" && (
                        <>
                            <div
                                className={
                                    globalConfig ? "d-flex flex-grow-1 justify-content-center" : "d-flex flex-grow-1"
                                }
                            ></div>
                            <Row className="mb-4 align-items-start">
                                {globalConfig && (
                                    <>
                                        <Col className="d-flex">
                                            <Input
                                                defaultValue={globalConfig.loadBalancerSeedValue}
                                                disabled
                                                placeholder="0 (default)"
                                            />
                                        </Col>
                                        {formValues.overrideConfig && formValues.loadBalancerSeedEnabled && (
                                            <GlobalSettingsSeparator />
                                        )}
                                    </>
                                )}
                                <Col className="d-flex align-items-center gap-3">
                                    <FormSwitch
                                        control={control}
                                        name="loadBalancerSeedEnabled"
                                        color="primary"
                                        label="Seed"
                                        disabled={
                                            formValues.loadBalancerValue !== "UseSessionContext" ||
                                            !formValues.overrideConfig
                                        }
                                        className="small"
                                    >
                                        Seed
                                        <i
                                            ref={popovers.setLoadBalanceSeedBehavior}
                                            className="cursor-default icon-info text-info margin-left-xxs"
                                        />
                                        <PopoverWithHover target={popovers.loadBalanceSeedBehavior} placement="top">
                                            <div className="flex-horizontal p-3">
                                                <div>
                                                    An optional seed number.
                                                    <br />
                                                    Used when hashing the session context.
                                                </div>
                                            </div>
                                        </PopoverWithHover>
                                    </FormSwitch>
                                    <InputGroup>
                                        <FormInput
                                            type="number"
                                            control={control}
                                            name="loadBalancerSeedValue"
                                            placeholder="0 (default)"
                                            disabled={!formValues.loadBalancerSeedEnabled || !formValues.overrideConfig}
                                        />
                                    </InputGroup>
                                </Col>
                            </Row>
                        </>
                    )}
                    <div className={globalConfig ? "d-flex flex-grow-1 justify-content-center" : "d-flex flex-grow-1"}>
                        <div className="md-label">
                            Read Balance Behavior{" "}
                            <i ref={popovers.setReadBalanceBehavior} className="icon-info text-info" />
                            <PopoverWithHover target={popovers.readBalanceBehavior} placement="top">
                                <div className="flex-horizontal p-3">
                                    <div>
                                        Set the Read balance method the client will use when accessing a node with{" "}
                                        <strong>Read</strong> requests.
                                        <br />
                                        <strong>Write</strong> requests are sent to the preferred node.
                                    </div>
                                </div>
                            </PopoverWithHover>
                        </div>
                    </div>
                    <Row className="align-items-start">
                        {globalConfig && (
                            <>
                                <Col className="d-flex">
                                    <Input
                                        defaultValue={globalConfig.readBalanceBehaviorValue}
                                        placeholder="None"
                                        disabled
                                    />
                                </Col>
                                {formValues.overrideConfig && formValues.readBalanceBehaviorEnabled && (
                                    <GlobalSettingsSeparator />
                                )}
                            </>
                        )}

                        <Col className="d-flex">
                            <InputGroup>
                                <InputGroupText>
                                    <FormCheckbox
                                        control={control}
                                        name="readBalanceBehaviorEnabled"
                                        disabled={!formValues.overrideConfig}
                                    />
                                </InputGroupText>
                                <FormSelect
                                    control={control}
                                    name="readBalanceBehaviorValue"
                                    disabled={!formValues.readBalanceBehaviorEnabled || !formValues.overrideConfig}
                                    options={ClientConfigurationUtils.getReadBalanceBehaviorOptions()}
                                />
                            </InputGroup>
                        </Col>
                    </Row>
                </Card>
            </Col>
        </Form>
    );
}

function GlobalSettingsSeparator() {
    return (
        <div className="align-self-center col-sm-auto d-flex">
            <Icon icon="arrow-right" margin="m-0" />
        </div>
    );
}

function getIdentityPartsSeparatorEffectiveValue(
    formValues: ClientConfigurationFormData,
    globalConfig: ClientConfigurationFormData
) {
    return (
        (formValues.overrideConfig && formValues.identityPartsSeparatorValue) ||
        globalConfig?.identityPartsSeparatorValue ||
        "'/' (Default)"
    );
}

function getMaximumNumberOfRequestsEffectiveValue(
    formValues: ClientConfigurationFormData,
    globalConfig: ClientConfigurationFormData
) {
    return (
        (formValues.overrideConfig && formValues.maximumNumberOfRequestsValue) ||
        globalConfig?.maximumNumberOfRequestsValue ||
        "30 (Default)"
    );
}

function getLoadBalancerEffectiveValue(
    formValues: ClientConfigurationFormData,
    globalConfig: ClientConfigurationFormData
) {
    return (
        (formValues.overrideConfig && formValues.loadBalancerEnabled && formValues.loadBalancerValue) ||
        (globalConfig?.loadBalancerEnabled && globalConfig?.loadBalancerValue) ||
        "None (Default)"
    );
}

function getLoadBalancerSeedEffectiveValue(
    formValues: ClientConfigurationFormData,
    globalConfig: ClientConfigurationFormData
) {
    return (
        (formValues.overrideConfig && formValues.loadBalancerSeedValue) ||
        globalConfig?.loadBalancerSeedValue ||
        "0 (Default)"
    );
}

function getReadBalanceBehaviorEffectiveValue(
    formValues: ClientConfigurationFormData,
    globalConfig: ClientConfigurationFormData
) {
    return (
        (formValues.overrideConfig && formValues.readBalanceBehaviorEnabled && formValues.readBalanceBehaviorValue) ||
        (globalConfig?.readBalanceBehaviorEnabled && globalConfig?.readBalanceBehaviorValue) ||
        "None (Default)"
    );
}
