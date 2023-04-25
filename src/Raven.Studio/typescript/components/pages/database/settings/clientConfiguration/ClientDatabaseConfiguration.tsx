import React, { useMemo } from "react";
import { Form, Col, Button, Card, Row, Spinner, Input } from "reactstrap";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormCheckbox, FormInput, FormSelect, FormSelectOption, FormSwitch } from "components/common/Form";
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;
import { useServices } from "components/hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { useAccessManager } from "hooks/useAccessManager";
import { LoadError } from "components/common/LoadError";
import genUtils = require("common/generalUtils");
import database = require("models/resources/database");
import {
    ClientConfigurationFormData,
    clientConfigurationYupResolver,
} from "components/common/clientConfiguration/ClientConfigurationValidation";
import { Icon } from "components/common/Icon";
import appUrl = require("common/appUrl");
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";

interface ClientDatabaseConfigurationProps {
    db: database;
}

// TODO: show modal on exit intent if is dirty
export default function ClientDatabaseConfiguration({ db }: ClientDatabaseConfigurationProps) {
    const { manageServerService } = useServices();
    const getClientConfigurationCallback = useAsyncCallback(manageServerService.getClientConfiguration);
    const getClientGlobalConfigurationCallback = useAsync(manageServerService.getGlobalClientConfiguration, []);

    const { isClusterAdminOrClusterNode: canNavigateToServerSettings } = useAccessManager();

    const { handleSubmit, control, formState, setValue, reset, getValues } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "onChange",
        defaultValues: async () =>
            ClientConfigurationUtils.mapToFormData(await getClientConfigurationCallback.execute(db)),
    });

    const globalConfig = useMemo(
        () => ClientConfigurationUtils.mapToFormData(getClientGlobalConfigurationCallback.result),
        [getClientGlobalConfigurationCallback.result]
    );

    const {
        overrideConfig,
        identityPartsSeparatorEnabled,
        maximumNumberOfRequestsEnabled,
        readBalanceBehaviorEnabled,
        useSessionContextEnabled,
        loadBalancerSeedEnabled,
        identityPartsSeparatorValue,
        maximumNumberOfRequestsValue,
    } = useWatch({ control });

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData) => {
        genUtils.tryHandleSubmit(async () => {
            await manageServerService.saveClientConfiguration(ClientConfigurationUtils.mapToDto(formData), db);
            reset(formData);
        });
    };

    if (getClientConfigurationCallback.loading || getClientGlobalConfigurationCallback.loading) {
        return <LoadingView />;
    }

    if (getClientConfigurationCallback.error) {
        return <LoadError error="Unable to load client configuration" />;
    }

    if (getClientGlobalConfigurationCallback.error) {
        return <LoadError error="Unable to load client global configuration" />;
    }

    return (
        <Form onSubmit={handleSubmit(onSave)}>
            <Col md={globalConfig ? 12 : 6} lg="12" className="p-4">
                <Button type="submit" color="primary" disabled={formState.isSubmitting || !formState.isDirty}>
                    {formState.isSubmitting ? <Spinner size="sm" className="me-1" /> : <i className="icon-save me-1" />}
                    Save
                </Button>

                {canNavigateToServerSettings() && (
                    <div className="d-flex justify-content-end">
                        <small title="Navigate to the server-wide Client Configuration View">
                            <a target="_blank" href={appUrl.forGlobalClientConfiguration()}>
                                <Icon icon="link" className="me-1" />
                                Go to Server-Wide Client Configuration View
                            </a>
                        </small>
                    </div>
                )}

                {globalConfig && (
                    <Card className="card flex-row p-2 mt-4">
                        <Row className="flex-grow-1">
                            <Col>
                                <FormSwitch control={control} name="overrideConfig" color="primary">
                                    Override Server Configuration
                                </FormSwitch>
                            </Col>
                            <Col>
                                <h3>Database Configuration</h3>
                            </Col>
                            <Col className="d-flex">
                                {canNavigateToServerSettings() && (
                                    <a target="_blank" href={appUrl.forGlobalClientConfiguration()} className="me-1">
                                        <Icon icon="link" />
                                    </a>
                                )}
                                <h3>Server Configuration</h3>
                            </Col>
                            <Col>
                                <h3>Effective Configuration</h3>
                            </Col>
                        </Row>
                    </Card>
                )}

                <Card className="card flex-row p-2 mt-4">
                    <Row className="flex-grow-1">
                        <Col>
                            <FormCheckbox
                                control={control}
                                name="identityPartsSeparatorEnabled"
                                afterChange={(event) =>
                                    !event.target.checked && setValue("identityPartsSeparatorValue", null)
                                }
                                disabled={!overrideConfig}
                            >
                                Identity parts separator
                            </FormCheckbox>
                        </Col>
                        <Col>
                            <FormInput
                                type="text"
                                control={control}
                                name="identityPartsSeparatorValue"
                                placeholder="Default is '/'"
                                disabled={!identityPartsSeparatorEnabled || !overrideConfig}
                            />
                        </Col>
                        {globalConfig && (
                            <>
                                <Col>
                                    <Input value={globalConfig.identityPartsSeparatorValue || undefined} disabled />
                                </Col>
                                <Col>
                                    <h3>
                                        {(overrideConfig && identityPartsSeparatorValue) ||
                                            globalConfig?.identityPartsSeparatorValue ||
                                            "'/' (Default)"}
                                    </h3>
                                </Col>
                            </>
                        )}
                    </Row>
                </Card>

                <Card className="flex-row mt-1 p-2">
                    <Row className="flex-grow-1">
                        <Col>
                            <FormCheckbox
                                control={control}
                                name="maximumNumberOfRequestsEnabled"
                                afterChange={(event) =>
                                    !event.target.checked && setValue("maximumNumberOfRequestsValue", null)
                                }
                                disabled={!overrideConfig}
                            >
                                Maximum number of requests per session
                            </FormCheckbox>
                        </Col>
                        <Col>
                            <FormInput
                                type="number"
                                control={control}
                                name="maximumNumberOfRequestsValue"
                                placeholder="Default value is 30"
                                disabled={!maximumNumberOfRequestsEnabled || !overrideConfig}
                            />
                        </Col>
                        {globalConfig && (
                            <>
                                <Col>
                                    <Input value={globalConfig.maximumNumberOfRequestsValue || undefined} disabled />
                                </Col>
                                <Col>
                                    <h3>
                                        {(overrideConfig && maximumNumberOfRequestsValue) ||
                                            globalConfig?.maximumNumberOfRequestsValue ||
                                            "30 (default)"}
                                    </h3>
                                </Col>
                            </>
                        )}
                    </Row>
                </Card>

                <Card className="mt-1">
                    <Row className="p-2">
                        <Col>
                            <FormCheckbox
                                control={control}
                                name="useSessionContextEnabled"
                                afterChange={(event) => {
                                    if (!event.target.checked) {
                                        setValue("loadBalancerSeedValue", null);
                                        setValue("loadBalancerSeedEnabled", false);
                                    }
                                }}
                                disabled={!overrideConfig}
                            >
                                Use Session Context for Load Balancing
                            </FormCheckbox>
                        </Col>
                        <Col>
                            <Row>
                                <Col>
                                    <FormSwitch
                                        control={control}
                                        name="loadBalancerSeedEnabled"
                                        color="primary"
                                        disabled={!useSessionContextEnabled || !overrideConfig}
                                        label="Seed"
                                        afterChange={(event) =>
                                            !event.target.checked && setValue("loadBalancerSeedValue", null)
                                        }
                                    >
                                        Seed
                                    </FormSwitch>
                                </Col>
                                <Col>
                                    <FormInput
                                        type="number"
                                        control={control}
                                        name="loadBalancerSeedValue"
                                        placeholder="Enter seed number"
                                        disabled={!loadBalancerSeedEnabled || !overrideConfig}
                                    />
                                </Col>
                            </Row>
                        </Col>
                        {globalConfig && (
                            <>
                                <Col>
                                    <Input value={globalConfig.loadBalancerSeedValue || undefined} disabled />
                                </Col>
                                <Col></Col>
                            </>
                        )}
                    </Row>

                    <Row className="p-2">
                        <Col>
                            <FormCheckbox
                                control={control}
                                name="readBalanceBehaviorEnabled"
                                afterChange={(event) =>
                                    !event.target.checked && setValue("readBalanceBehaviorValue", "None")
                                }
                                disabled={!overrideConfig}
                            >
                                Read balance behavior
                            </FormCheckbox>
                        </Col>
                        <Col>
                            <FormSelect
                                control={control}
                                name="readBalanceBehaviorValue"
                                disabled={!readBalanceBehaviorEnabled || !overrideConfig}
                            >
                                <FormSelectOption<ReadBalanceBehavior> label="None" value="None" />
                                <FormSelectOption<ReadBalanceBehavior> label="Round Robin" value="RoundRobin" />
                                <FormSelectOption<ReadBalanceBehavior> label="Fastest Node" value="FastestNode" />
                            </FormSelect>
                        </Col>
                        {globalConfig && (
                            <>
                                <Col>
                                    <Input value={globalConfig.readBalanceBehaviorValue || undefined} disabled />
                                </Col>
                                <Col>
                                    <h3>{getEffectiveReadBalance(overrideConfig, getValues(), globalConfig)}</h3>
                                </Col>
                            </>
                        )}
                    </Row>
                </Card>
            </Col>
        </Form>
    );
}

function getEffectiveReadBalance(
    overrideConfig: boolean,
    databaseConfig: ClientConfigurationFormData,
    globalConfig: ClientConfigurationFormData
): string {
    if (overrideConfig) {
        return getEffectiveReadBalanceForConfig(databaseConfig);
    }

    return getEffectiveReadBalanceForConfig(globalConfig);
}

function getEffectiveReadBalanceForConfig(config: ClientConfigurationFormData) {
    if (config.useSessionContextEnabled) {
        if (config.loadBalancerSeedValue) {
            return `Session Context (Seed: ${config.loadBalancerSeedValue})`;
        }
        return "Session Context";
    }

    if (config.readBalanceBehaviorEnabled) {
        return config.readBalanceBehaviorValue;
    }

    return "None (default)";
}
