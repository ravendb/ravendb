import React, { useEffect } from "react";
import { Form, Col, Button, Card, Row, Spinner } from "reactstrap";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormCheckbox, FormInput, FormSelectOption, FormSwitch } from "components/common/Form";
import { ClientConfigurationFormData, clientConfigurationYupResolver } from "./ClientConfigurationValidation";
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import { useServices } from "components/hooks/useServices";
import { useAsync } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";

// TODO: server wide
export default function ClientConfiguration() {
    const { manageServerService } = useServices();
    const { result, loading } = useAsync(manageServerService.getGlobalClientConfiguration, []);

    const { handleSubmit, control, resetField, formState, setValue } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "onChange",
    });

    useEffect(() => {
        if (!result) {
            return;
        }

        setValue("identityPartsSeparatorEnabled", !!result.IdentityPartsSeparator);
        setValue("identityPartsSeparatorValue", result.IdentityPartsSeparator);
        setValue("maximumNumberOfRequestsEnabled", !!result.MaxNumberOfRequestsPerSession);
        setValue("maximumNumberOfRequestsValue", result.MaxNumberOfRequestsPerSession);
        setValue("useSessionContextEnabled", result.LoadBalanceBehavior === "UseSessionContext");
        setValue("loadBalancerSeedEnabled", !!result.LoadBalancerContextSeed);
        setValue("loadBalancerSeedValue", result.LoadBalancerContextSeed);
        setValue("readBalanceBehaviorEnabled", result.ReadBalanceBehavior !== "None");
        setValue("readBalanceBehaviorValue", result.ReadBalanceBehavior);
    }, [setValue, result]);

    const {
        identityPartsSeparatorEnabled,
        maximumNumberOfRequestsEnabled,
        readBalanceBehaviorEnabled,
        useSessionContextEnabled,
        loadBalancerSeedEnabled,
    } = useWatch({ control });

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData): Promise<void> => {
        manageServerService.saveGlobalClientConfiguration(toDto(formData, result?.Disabled)).catch(() => {
            // empty by design
        });
    };

    if (loading) {
        return <LoadingView />;
    }
    // TODO: show modal on exit if is dirty
    return (
        <Form onSubmit={handleSubmit(onSave)}>
            <Col md={6} className="p-4">
                <Button type="submit" color="primary" disabled={formState.isSubmitting || !formState.isDirty}>
                    {formState.isSubmitting ? <Spinner size="sm" className="me-1" /> : <i className="icon-save me-1" />}
                    Save
                </Button>

                <Card className="card flex-row p-2 mt-4">
                    <Row className="flex-grow-1">
                        <Col>
                            <FormCheckbox
                                type="checkbox"
                                control={control}
                                name="identityPartsSeparatorEnabled"
                                afterChange={(event) =>
                                    !event.target.checked && resetField("identityPartsSeparatorValue")
                                }
                            >
                                Identity parts separator
                            </FormCheckbox>
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
                            <FormCheckbox
                                type="checkbox"
                                control={control}
                                name="maximumNumberOfRequestsEnabled"
                                afterChange={(event) =>
                                    !event.target.checked && resetField("maximumNumberOfRequestsValue")
                                }
                            >
                                Maximum number of requests per session
                            </FormCheckbox>
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
                            <FormCheckbox
                                type="checkbox"
                                control={control}
                                name="useSessionContextEnabled"
                                afterChange={(event) => {
                                    if (!event.target.checked) {
                                        resetField("loadBalancerSeedValue");
                                        resetField("loadBalancerSeedEnabled");
                                    }
                                }}
                            >
                                Use Session Context for Load Balancing
                            </FormCheckbox>
                        </Col>
                        <Col md={5}>
                            <Row>
                                <Col>
                                    <FormSwitch
                                        type="switch"
                                        control={control}
                                        name="loadBalancerSeedEnabled"
                                        disabled={!useSessionContextEnabled}
                                        label="Seed"
                                        afterChange={(event) =>
                                            !event.target.checked && resetField("loadBalancerSeedValue")
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
                                        disabled={!loadBalancerSeedEnabled}
                                    />
                                </Col>
                            </Row>
                        </Col>
                    </Row>

                    <Row className="p-2">
                        <Col>
                            <FormCheckbox
                                type="checkbox"
                                control={control}
                                name="readBalanceBehaviorEnabled"
                                afterChange={(event) => !event.target.checked && resetField("readBalanceBehaviorValue")}
                            >
                                Read balance behavior
                            </FormCheckbox>
                        </Col>
                        <Col md={5}>
                            <FormInput
                                type="select"
                                control={control}
                                name="readBalanceBehaviorValue"
                                disabled={!readBalanceBehaviorEnabled || useSessionContextEnabled}
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

function toDto(formData: ClientConfigurationFormData, disabled: boolean): ClientConfiguration {
    return {
        IdentityPartsSeparator: formData.identityPartsSeparatorValue,
        LoadBalanceBehavior: formData.useSessionContextEnabled ? "UseSessionContext" : "None",
        LoadBalancerContextSeed: formData.loadBalancerSeedValue,
        ReadBalanceBehavior: formData.readBalanceBehaviorValue,
        MaxNumberOfRequestsPerSession: formData.maximumNumberOfRequestsValue,
        // TODO for the database view
        Disabled: disabled,
        Etag: undefined,
    };
}
