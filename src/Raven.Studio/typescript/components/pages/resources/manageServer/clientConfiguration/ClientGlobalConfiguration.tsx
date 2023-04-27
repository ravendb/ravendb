import React from "react";
import { Form, Col, Button, Card, Row, Spinner } from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormCheckbox, FormInput, FormSelect, FormSwitch } from "components/common/Form";
import {
    ClientConfigurationFormData,
    clientConfigurationYupResolver,
} from "../../../../common/clientConfiguration/ClientConfigurationValidation";
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";
import useClientConfigurationFormController from "components/common/clientConfiguration/useClientConfigurationFormController";
import { tryHandleSubmit } from "components/utils/common";

// TODO: show modal on exit intent if is dirty
export default function ClientGlobalConfiguration() {
    const { manageServerService } = useServices();
    const getGlobalClientConfigurationCallback = useAsyncCallback(manageServerService.getGlobalClientConfiguration);

    const { handleSubmit, control, formState, setValue, reset } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "onChange",
        defaultValues: async () =>
            ClientConfigurationUtils.mapToFormData(await getGlobalClientConfigurationCallback.execute(), true),
    });

    const formValues = useClientConfigurationFormController(control, setValue);

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveGlobalClientConfiguration(ClientConfigurationUtils.mapToDto(formData, true));
            reset(null, { keepValues: true });
        });
    };

    const onRefresh = async () => {
        reset(ClientConfigurationUtils.mapToFormData(await getGlobalClientConfigurationCallback.execute(), true));
    };

    if (getGlobalClientConfigurationCallback.loading) {
        return <LoadingView />;
    }

    if (getGlobalClientConfigurationCallback.error) {
        return <LoadError error="Unable to load client global configuration" refresh={onRefresh} />;
    }

    return (
        <Form onSubmit={handleSubmit(onSave)}>
            <Col md="12" lg="6" className="p-4">
                <Button type="submit" color="primary" disabled={formState.isSubmitting || !formState.isDirty}>
                    {formState.isSubmitting ? <Spinner size="sm" className="me-1" /> : <i className="icon-save me-1" />}
                    Save
                </Button>

                <Card className="card flex-row p-2 mt-4">
                    <Row className="flex-grow-1">
                        <Col>
                            <FormCheckbox control={control} name="identityPartsSeparatorEnabled">
                                Identity parts separator
                            </FormCheckbox>
                        </Col>
                        <Col>
                            <FormInput
                                type="text"
                                control={control}
                                name="identityPartsSeparatorValue"
                                placeholder="Default is '/'"
                                disabled={!formValues.identityPartsSeparatorEnabled}
                            />
                        </Col>
                    </Row>
                </Card>

                <Card className="flex-row mt-1 p-2">
                    <Row className="flex-grow-1">
                        <Col>
                            <FormCheckbox control={control} name="maximumNumberOfRequestsEnabled">
                                Maximum number of requests per session
                            </FormCheckbox>
                        </Col>
                        <Col>
                            <FormInput
                                type="number"
                                control={control}
                                name="maximumNumberOfRequestsValue"
                                placeholder="Default value is 30"
                                disabled={!formValues.maximumNumberOfRequestsEnabled}
                            />
                        </Col>
                    </Row>
                </Card>

                <Card className="mt-1">
                    <Row className="p-2">
                        <Col>
                            <FormCheckbox control={control} name="useSessionContextEnabled">
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
                                        disabled={!formValues.useSessionContextEnabled}
                                        label="Seed"
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
                                        disabled={!formValues.loadBalancerSeedEnabled}
                                    />
                                </Col>
                            </Row>
                        </Col>
                    </Row>

                    <Row className="p-2">
                        <Col>
                            <FormCheckbox control={control} name="readBalanceBehaviorEnabled">
                                Read balance behavior
                            </FormCheckbox>
                        </Col>
                        <Col>
                            <FormSelect<ReadBalanceBehavior>
                                control={control}
                                name="readBalanceBehaviorValue"
                                disabled={!formValues.readBalanceBehaviorEnabled}
                                options={ClientConfigurationUtils.getReadBalanceBehaviorOptions()}
                            />
                        </Col>
                    </Row>
                </Card>
            </Col>
        </Form>
    );
}
