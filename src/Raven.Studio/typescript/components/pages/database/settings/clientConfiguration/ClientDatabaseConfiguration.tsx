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
        mode: "onChange",
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

    function GlobalSettingsSeparator() {
        return (
            <>
                <div className="align-self-center col-sm-auto d-flex">
                    <Icon icon="arrow-right" margin="m-0" />
                </div>
            </>
        );
    }

    return (
        <Form onSubmit={handleSubmit(onSave)}>
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
                        {formState.isDirty && globalConfig && (
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
                                        <PropSummaryValue color="info">a</PropSummaryValue>
                                    </PropSummaryItem>
                                    <PropSummaryItem>
                                        <PropSummaryName>Max number of requests per session</PropSummaryName>
                                        <PropSummaryValue color="info">b</PropSummaryValue>
                                    </PropSummaryItem>
                                    <PropSummaryItem>
                                        <PropSummaryName>Load Balance Behavior</PropSummaryName>
                                        <PropSummaryValue color="info">c</PropSummaryValue>
                                    </PropSummaryItem>
                                    <PropSummaryItem>
                                        <PropSummaryName>Seed</PropSummaryName>
                                        <PropSummaryValue color="info">d</PropSummaryValue>
                                    </PropSummaryItem>
                                    <PropSummaryItem>
                                        <PropSummaryName>Read Balance Behavior</PropSummaryName>
                                        <PropSummaryValue color="info">e</PropSummaryValue>
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
                                    Changes the default separator for automatically generated document IDs. You can use
                                    any <code>char</code> except <code>|</code> (pipe)
                                </div>
                            </div>
                        </PopoverWithHover>
                    </div>
                    <Row className="flex-grow-1 align-items-start">
                        {globalConfig && (
                            <>
                                <Col className="d-flex">
                                    <Input defaultValue={globalConfig.identityPartsSeparatorValue} disabled />
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
                                    placeholder="Default is '/'"
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
                                    <Input defaultValue={globalConfig.maximumNumberOfRequestsValue} disabled />
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
                                    placeholder="Default value is 30"
                                    disabled={!formValues.maximumNumberOfRequestsEnabled || !formValues.overrideConfig}
                                />
                            </InputGroup>
                        </Col>
                    </Row>
                </Card>
                <h4 className={globalConfig ? "mt-4 text-center" : "mt-4"}>Load Balancing Client Requests</h4>
                <Card className="flex-column p-3">
                    <div className={globalConfig ? "d-flex flex-grow-1 justify-content-center" : "d-flex flex-grow-1"}>
                        <div className="md-label">
                            Load Balance Behavior <i ref={popovers.setSessionContext} className="icon-info text-info" />
                            <PopoverWithHover target={popovers.sessionContext} placement="top">
                                <div className="flex-horizontal p-3">
                                    <div>
                                        Allow client sessions to select topology by tag, so they&apos;d be able to
                                        load-balance their requests.
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
                                                placeholder="0"
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
                                            className="icon-info text-info margin-left-xxs"
                                        />
                                        <PopoverWithHover target={popovers.loadBalanceSeedBehavior} placement="top">
                                            <div className="flex-horizontal p-3">
                                                <div>
                                                    Select a hash seed to fix the topology that clients would use.
                                                </div>
                                            </div>
                                        </PopoverWithHover>
                                    </FormSwitch>
                                    <InputGroup>
                                        <FormInput
                                            type="number"
                                            control={control}
                                            name="loadBalancerSeedValue"
                                            placeholder="Enter seed number"
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
                                        Set the load-balance method that the client will use when accessing a node with
                                        <strong> Read</strong> requests. The method selected will also affect the
                                        client&apos;s decision of which node to failover to in case of issues with the
                                        <strong> Read</strong> request.
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
