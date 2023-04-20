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
import { LoadError } from "components/common/LoadError";

// TODO: server wide
export default function ClientConfiguration() {
    const { manageServerService } = useServices();
    const { result, loading, error } = useAsync(manageServerService.getGlobalClientConfiguration, []);

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
        setValue("sessionContextEnabled", result.LoadBalanceBehavior !== "None");
        setValue("seedEnabled", !!result.LoadBalancerContextSeed);
        setValue("seedValue", result.LoadBalancerContextSeed);
        setValue("readBalanceBehaviorEnabled", result.ReadBalanceBehavior !== "None");
        setValue("readBalanceBehaviorValue", result.ReadBalanceBehavior);
    }, [setValue, result]);

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
                <Button type="submit" color="primary" disabled={formState.isSubmitting}>
                    {formState.isSubmitting ? <Spinner size="sm" /> : <i className="icon-save margin-right-xxs" />}
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
                                name="sessionContextEnabled"
                                afterChange={(event) => {
                                    if (!event.target.checked) {
                                        resetField("seedValue");
                                        resetField("seedEnabled");
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
                                        name="seedEnabled"
                                        disabled={!sessionContextEnabled}
                                        label="Seed"
                                        afterChange={(event) => !event.target.checked && resetField("seedValue")}
                                    >
                                        Seed
                                    </FormSwitch>
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
                                disabled={!readBalanceBehaviorEnabled || sessionContextEnabled}
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
        LoadBalanceBehavior: formData.sessionContextEnabled ? "UseSessionContext" : "None",
        LoadBalancerContextSeed: formData.seedValue,
        ReadBalanceBehavior: formData.readBalanceBehaviorValue,
        MaxNumberOfRequestsPerSession: formData.maximumNumberOfRequestsValue,
        Disabled: disabled,
        Etag: undefined,
    };
}
