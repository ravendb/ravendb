import React, { useEffect, useMemo } from "react";
import { Form, Col, Button, Card, Row, Spinner } from "reactstrap";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormInput, FormSelectOption, FormToggle } from "components/utils/FormUtils";
import { ClientConfigurationFormData, clientConfigurationYupResolver } from "./ClientConfigurationValidation";
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import { useServices } from "components/hooks/useServices";
import { useAsync } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";

// TODO: server wide
export default function ClientConfiguration() {
    const { manageServerService } = useServices();
    const { result, loading, error } = useAsync(manageServerService.getGlobalClientConfiguration, []);

    const { handleSubmit, control, resetField, formState, reset } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
    });

    useEffect(() => {
        const resultFormData = getDefaultValues(result);
        if (resultFormData) {
            reset(resultFormData);
        }
    }, [reset, result]);

    const {
        identityPartsSeparatorEnabled,
        maximumNumberOfRequestsEnabled,
        readBalanceBehaviorEnabled,
        sessionContextEnabled,
        seedEnabled,
    } = useWatch({ control });

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData) => {
        await manageServerService.saveGlobalClientConfiguration(toDto(formData, result?.Disabled));
    };

    if (loading) {
        return <LoadingView />;
    }

    if (error) {
        return <LoadError />;
    }

    return (
        <Form onSubmit={handleSubmit(onSave)}>
            <Col md={6} className="p-4">
                {/* TODO: add spinner on save */}
                <Button type="submit" color="primary" disabled={formState.isSubmitting}>
                    {formState.isSubmitting ? <Spinner size="sm" /> : <i className="icon-save margin-right-xxs" />}
                    Save
                </Button>

                <Card className="card flex-row p-2 mt-4">
                    <Row className="flex-grow-1">
                        <Col>
                            <FormToggle
                                type="checkbox"
                                control={control}
                                name="identityPartsSeparatorEnabled"
                                label="Identity parts separator"
                                afterChange={(event) =>
                                    !event.target.checked && resetField("identityPartsSeparatorValue")
                                }
                            />
                        </Col>
                        <Col md={5}>
                            <FormInput
                                type="text"
                                control={control}
                                name="identityPartsSeparatorValue"
                                placeholder="Default is '/'"
                                disabled={!identityPartsSeparatorEnabled}
                            />
                        </Col>
                    </Row>
                </Card>

                <Card className="flex-row mt-1 p-2">
                    <Row className="flex-grow-1">
                        <Col>
                            <FormToggle
                                type="checkbox"
                                control={control}
                                name="maximumNumberOfRequestsEnabled"
                                label="Maximum number of requests per session"
                                afterChange={(event) =>
                                    !event.target.checked && resetField("maximumNumberOfRequestsValue")
                                }
                            />
                        </Col>
                        <Col lg={5}>
                            <FormInput
                                type="number"
                                control={control}
                                name="maximumNumberOfRequestsValue"
                                placeholder="Default value is 30"
                                disabled={!maximumNumberOfRequestsEnabled}
                            />
                        </Col>
                    </Row>
                </Card>

                <Card className="mt-1">
                    <Row className="p-2">
                        <Col>
                            <FormToggle
                                type="checkbox"
                                control={control}
                                name="sessionContextEnabled"
                                label="Use Session Context for Load Balancing"
                                afterChange={(event) => {
                                    if (!event.target.checked) {
                                        resetField("seedValue");
                                        resetField("seedEnabled");
                                    }
                                }}
                            />
                        </Col>
                        <Col md={5}>
                            <Row>
                                <Col>
                                    <FormToggle
                                        type="switch"
                                        control={control}
                                        name="seedEnabled"
                                        disabled={!sessionContextEnabled}
                                        label="Seed"
                                        afterChange={(event) => !event.target.checked && resetField("seedValue")}
                                    />
                                </Col>
                                <Col>
                                    <FormInput
                                        type="number"
                                        control={control}
                                        name="seedValue"
                                        placeholder="Enter seed number"
                                        disabled={!seedEnabled}
                                    />
                                </Col>
                            </Row>
                        </Col>
                    </Row>

                    <Row className="p-2">
                        <Col>
                            <FormToggle
                                type="checkbox"
                                control={control}
                                name="readBalanceBehaviorEnabled"
                                label="Read balance behavior"
                                afterChange={(event) => !event.target.checked && resetField("readBalanceBehaviorValue")}
                            />
                        </Col>
                        <Col md={5}>
                            <FormInput
                                control={control}
                                name="readBalanceBehaviorValue"
                                type="select"
                                disabled={!readBalanceBehaviorEnabled}
                            >
                                <FormSelectOption<ReadBalanceBehavior> label="None" value="None" />
                                <FormSelectOption<ReadBalanceBehavior> label="Round Robin" value="RoundRobin" />
                                <FormSelectOption<ReadBalanceBehavior> label="Fastest Node" value="FastestNode" />
                            </FormInput>
                        </Col>
                    </Row>
                </Card>
            </Col>
        </Form>
    );
}

function getDefaultValues(dto: ClientConfiguration) {
    if (!dto) {
        return null;
    }

    return {
        identityPartsSeparatorEnabled: !!dto.IdentityPartsSeparator,
        identityPartsSeparatorValue: dto.IdentityPartsSeparator,
        maximumNumberOfRequestsEnabled: !!dto.MaxNumberOfRequestsPerSession,
        maximumNumberOfRequestsValue: dto.MaxNumberOfRequestsPerSession,
        sessionContextEnabled: dto.LoadBalanceBehavior !== "None",
        seedEnabled: !!dto.LoadBalancerContextSeed,
        seedValue: dto.LoadBalancerContextSeed,
        readBalanceBehaviorEnabled: dto.ReadBalanceBehavior !== "None",
        readBalanceBehaviorValue: dto.ReadBalanceBehavior,
    };
}

function toDto(formData: ClientConfigurationFormData, disabled: boolean): ClientConfiguration {
    return {
        IdentityPartsSeparator: formData.identityPartsSeparatorValue,
        LoadBalanceBehavior: formData.sessionContextEnabled ? "UseSessionContext" : "None",
        LoadBalancerContextSeed: formData.seedValue,
        ReadBalanceBehavior: formData.readBalanceBehaviorValue,
        MaxNumberOfRequestsPerSession: formData.maximumNumberOfRequestsValue,
        Disabled: disabled,
        Etag: undefined,
    };
}
