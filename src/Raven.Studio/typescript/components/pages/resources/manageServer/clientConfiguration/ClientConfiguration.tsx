import React, { useEffect } from "react";
import { Form, FormGroup, Col, Button, Card, Row } from "reactstrap";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormCheckbox, FormInput, FormSelectOption, FormSwitch } from "components/utils/FormUtils";
import { ClientConfigurationFormData, clientConfigurationYupResolver } from "./ClientConfigurationValidation";
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;

// TODO: server wide
export default function ClientConfiguration() {
    const { handleSubmit, control, watch, resetField } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        defaultValues: {
            readBalanceBehaviorValue: "None",
        },
    });

    const [
        identityPartsSeparatorEnabled,
        maximumNumberOfRequestsEnabled,
        readBalanceBehaviorEnabled,
        sessionContextEnabled,
        seedEnabled,
    ] = watch([
        "identityPartsSeparatorEnabled",
        "maximumNumberOfRequestsEnabled",
        "readBalanceBehaviorEnabled",
        "sessionContextEnabled",
        "seedEnabled",
    ]);

    useEffect(() => {
        if (!identityPartsSeparatorEnabled) {
            resetField("identityPartsSeparatorValue");
        }
        if (!maximumNumberOfRequestsEnabled) {
            resetField("maximumNumberOfRequestsValue");
        }
        if (!readBalanceBehaviorEnabled) {
            resetField("readBalanceBehaviorValue");
        }
        if (!sessionContextEnabled) {
            resetField("seedEnabled");
        }
        if (!seedEnabled) {
            resetField("seedValue");
        }
    }, [
        resetField,
        identityPartsSeparatorEnabled,
        maximumNumberOfRequestsEnabled,
        readBalanceBehaviorEnabled,
        seedEnabled,
        sessionContextEnabled,
    ]);

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
                            <FormCheckbox
                                control={control}
                                name="identityPartsSeparatorEnabled"
                                label="Identity parts separator"
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
                            <FormCheckbox
                                control={control}
                                name="maximumNumberOfRequestsEnabled"
                                label="Maximum number of requests per session"
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
                            <FormCheckbox
                                control={control}
                                name="sessionContextEnabled"
                                label="Use Session Context for Load Balancing"
                            />
                        </Col>
                        <Col>
                            <Row>
                                <Col>
                                    {/* // TODO: check if disabled is in type */}
                                    <FormSwitch
                                        control={control}
                                        name="seedEnabled"
                                        type="switch"
                                        disabled={!sessionContextEnabled}
                                        label="Seed"
                                    />
                                </Col>
                                <Col>
                                    {/* // TODO: fix number validation if null */}
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
                            <FormCheckbox
                                control={control}
                                name="readBalanceBehaviorEnabled"
                                label="Read balance behavior"
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
