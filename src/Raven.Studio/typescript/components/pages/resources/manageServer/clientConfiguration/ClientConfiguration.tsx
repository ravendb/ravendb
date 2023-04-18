import React from "react";
import { Form, FormGroup, Col, Button, Card, Row } from "reactstrap";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormInput, FormSelectOption, FormToggle } from "components/utils/FormUtils";
import { ClientConfigurationFormData, clientConfigurationYupResolver } from "./ClientConfigurationValidation";
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;

// TODO: server wide
export default function ClientConfiguration() {
    const { handleSubmit, control, resetField } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
    });

    const {
        identityPartsSeparatorEnabled,
        maximumNumberOfRequestsEnabled,
        readBalanceBehaviorEnabled,
        sessionContextEnabled,
        seedEnabled,
    } = useWatch({ control });

    // TODO: logic
    const onSave: SubmitHandler<ClientConfigurationFormData> = (data) => {
        console.log(data);
    };

    return (
        <Form onSubmit={handleSubmit(onSave)}>
            <Col md={6}>
                {/* TODO: add spinner on save */}
                <Button type="submit" color="primary">
                    <i className="icon-save margin-right-xxs" />
                    Save
                </Button>

                <Card className="p-4 mt-4">
                    <FormGroup className="flex-horizontal">
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
                        <Col>
                            <FormInput
                                type="text"
                                control={control}
                                name="identityPartsSeparatorValue"
                                placeholder="Default is '/'"
                                disabled={!identityPartsSeparatorEnabled}
                            />
                        </Col>
                    </FormGroup>

                    <FormGroup className="flex-horizontal">
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
                        <Col>
                            <FormInput
                                type="number"
                                control={control}
                                name="maximumNumberOfRequestsValue"
                                placeholder="Default value is 30"
                                disabled={!maximumNumberOfRequestsEnabled}
                            />
                        </Col>
                    </FormGroup>

                    <FormGroup className="flex-horizontal">
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
                        <Col>
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
                    </FormGroup>

                    <FormGroup className="flex-horizontal">
                        <Col>
                            <FormToggle
                                type="checkbox"
                                control={control}
                                name="readBalanceBehaviorEnabled"
                                label="Read balance behavior"
                                afterChange={(event) => !event.target.checked && resetField("readBalanceBehaviorValue")}
                            />
                        </Col>
                        <Col>
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
                    </FormGroup>
                </Card>
            </Col>
        </Form>
    );
}
