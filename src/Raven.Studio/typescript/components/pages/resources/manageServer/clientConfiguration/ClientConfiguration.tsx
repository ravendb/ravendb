import React from "react";
import { Form, Col, Button, Card, Row, Spinner } from "reactstrap";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormCheckbox, FormInput, FormSelect, FormSelectOption, FormSwitch } from "components/common/Form";
import { ClientConfigurationFormData, clientConfigurationYupResolver } from "./ClientConfigurationValidation";
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";

// TODO: server wide
export default function ClientConfiguration() {
    const { manageServerService } = useServices();
    const {
        execute: executeGetGlobalClientConfiguration,
        result: clientConfigurationDto,
        error: clientConfigurationError,
        loading: isLoadingClientConfiguration,
    } = useAsyncCallback(manageServerService.getGlobalClientConfiguration);

    const { handleSubmit, control, formState, setValue } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "onChange",
        defaultValues: async () => getDefaultFormValues(await executeGetGlobalClientConfiguration()),
    });

    const {
        identityPartsSeparatorEnabled,
        maximumNumberOfRequestsEnabled,
        readBalanceBehaviorEnabled,
        useSessionContextEnabled,
        loadBalancerSeedEnabled,
    } = useWatch({ control });

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData): Promise<void> => {
        manageServerService
            .saveGlobalClientConfiguration(toDto(formData, clientConfigurationDto?.Disabled))
            .catch(() => {
                // empty by design
            });
    };

    if (isLoadingClientConfiguration) {
        return <LoadingView />;
    }

    if (clientConfigurationError) {
        return <LoadError />;
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
                                    !event.target.checked && setValue("identityPartsSeparatorValue", null)
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
                                    !event.target.checked && setValue("maximumNumberOfRequestsValue", null)
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
                                        setValue("loadBalancerSeedValue", null);
                                        setValue("loadBalancerSeedEnabled", false);
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
                                afterChange={(event) =>
                                    !event.target.checked && setValue("readBalanceBehaviorValue", null)
                                }
                            >
                                Read balance behavior
                            </FormCheckbox>
                        </Col>
                        <Col md={5}>
                            <FormSelect
                                control={control}
                                name="readBalanceBehaviorValue"
                                disabled={!readBalanceBehaviorEnabled || useSessionContextEnabled}
                            >
                                <FormSelectOption<ReadBalanceBehavior> label="None" value="None" />
                                <FormSelectOption<ReadBalanceBehavior> label="Round Robin" value="RoundRobin" />
                                <FormSelectOption<ReadBalanceBehavior> label="Fastest Node" value="FastestNode" />
                            </FormSelect>
                        </Col>
                    </Row>
                </Card>
            </Col>
        </Form>
    );
}

function getDefaultFormValues(dto: ClientConfiguration) {
    console.log("getDefaultFormValues");
    if (!dto) {
        return null;
    }

    return {
        identityPartsSeparatorEnabled: !!dto.IdentityPartsSeparator,
        identityPartsSeparatorValue: dto.IdentityPartsSeparator,
        maximumNumberOfRequestsEnabled: !!dto.MaxNumberOfRequestsPerSession,
        maximumNumberOfRequestsValue: dto.MaxNumberOfRequestsPerSession,
        useSessionContextEnabled: dto.LoadBalanceBehavior === "UseSessionContext",
        loadBalancerSeedEnabled: !!dto.LoadBalancerContextSeed,
        loadBalancerSeedValue: dto.LoadBalancerContextSeed,
        readBalanceBehaviorEnabled: dto.ReadBalanceBehavior !== "None",
        readBalanceBehaviorValue: dto.ReadBalanceBehavior,
    };
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
