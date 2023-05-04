import React, { useState } from "react";
import { Form, Col, Button, Card, Row, Spinner, InputGroup, InputGroupText } from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormCheckbox, FormInput, FormSelect, FormSwitch } from "components/common/Form";
import {
    ClientConfigurationFormData,
    clientConfigurationYupResolver,
} from "../../../../common/clientConfiguration/ClientConfigurationValidation";
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;
import LoadBalanceBehavior = Raven.Client.Http.LoadBalanceBehavior;
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";
import useClientConfigurationFormController from "components/common/clientConfiguration/useClientConfigurationFormController";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import { PopoverWithHover } from "components/common/PopoverWithHover";

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

    const [identityPartsSeparatorPopover, setIdentityPartsSeparatorPopover] = useState<HTMLElement>();
    const [maximumRequestsPerSessionPopover, setMaximumRequestsPerSessionPopover] = useState<HTMLElement>();
    const [sessionContextPopover, setSessionContextPopover] = useState<HTMLElement>();
    const [readBalanceBehaviorPopover, setReadBalanceBehaviorPopover] = useState<HTMLElement>();
    const [loadBalanceSeedBehaviorPopover, setLoadBalanceSeedBehaviorPopover] = useState<HTMLElement>();

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
            <Col md="6" lg="4" className="content-margin">
                <Button type="submit" color="primary" disabled={formState.isSubmitting || !formState.isDirty}>
                    {formState.isSubmitting ? <Spinner size="sm" className="me-1" /> : <Icon icon="save" />}
                    Save
                </Button>

                <Card className="card flex-column p-3 my-3">
                    <div className="d-flex flex-grow-1">
                        <div className="md-label">
                            Identity parts separator{" "}
                            <i ref={setIdentityPartsSeparatorPopover} className="icon-info text-info" />
                        </div>
                        <PopoverWithHover target={identityPartsSeparatorPopover} placement="top">
                            <div className="flex-horizontal p-3">
                                <div>
                                    Changes the default separator for automatically generated document IDs. You can use
                                    any <code>char</code> except <code>|</code> (pipe)
                                </div>
                            </div>
                        </PopoverWithHover>
                    </div>
                    <Row className="flex-grow-1 mt-1">
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

                <Card className="flex-column mt-1 mb-3 p-3">
                    <div className="d-flex flex-grow-1">
                        <div className="md-label">
                            Maximum number of requests per session{" "}
                            <i ref={setMaximumRequestsPerSessionPopover} className="icon-info text-info" />
                        </div>
                        <PopoverWithHover target={maximumRequestsPerSessionPopover} placement="top">
                            <div className="flex-horizontal p-3">
                                <div>
                                    Set this number to restrict the number of requests (<strong>Reads</strong> &{" "}
                                    <strong>Writes</strong>) per session in the client API.
                                </div>
                            </div>
                        </PopoverWithHover>
                    </div>
                    <Row className="flex-grow-1 mt-1">
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
                <Card className="flex-column mt-1 p-3">
                    <div className="d-flex flex-grow-1">
                        <div className="md-label">
                            Load Balancing <i ref={setSessionContextPopover} className="icon-info text-info" />
                            <PopoverWithHover target={sessionContextPopover} placement="top">
                                <div className="flex-horizontal p-3">
                                    <div>
                                        Allow client sessions to select topology by tag, so they'd be able to
                                        load-balance their requests.
                                    </div>
                                </div>
                            </PopoverWithHover>
                        </div>
                    </div>
                    <Row className="mt-1 mb-4">
                        <Col className="d-flex align-items-center gap-3">
                            <InputGroup>
                                <InputGroupText>
                                    <FormCheckbox
                                        control={control}
                                        name="useSessionContextEnabled"
                                        disabled={!formValues.overrideConfig}
                                    />
                                </InputGroupText>
                                <FormSelect<LoadBalanceBehavior>
                                    control={control}
                                    name="loadBalanceBehaviorValue"
                                    disabled={!formValues.useSessionContextEnabled || !formValues.overrideConfig}
                                    options={ClientConfigurationUtils.getLoadBalanceBehaviorOptions()}
                                />
                            </InputGroup>
                        </Col>
                    </Row>
                    {formValues.useSessionContextEnabled && (
                        <>
                            <div className="d-flex flex-grow-1">
                                <div className="md-label">
                                    Hash seed{" "}
                                    <i ref={setLoadBalanceSeedBehaviorPopover} className="icon-info text-info" />
                                    <PopoverWithHover target={loadBalanceSeedBehaviorPopover} placement="top">
                                        <div className="flex-horizontal p-3">
                                            <div>Select a hash seed to fix the topology that clients would use.</div>
                                        </div>
                                    </PopoverWithHover>
                                </div>
                            </div>
                            <Row className="mt-1 mb-4">
                                <Col className="d-flex align-items-center gap-3">
                                    <FormSwitch
                                        control={control}
                                        name="loadBalancerSeedEnabled"
                                        color="primary"
                                        disabled={!formValues.useSessionContextEnabled || !formValues.overrideConfig}
                                        label="Seed"
                                    >
                                        Seed
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
                    <div className="d-flex flex-grow-1">
                        <div className="md-label">
                            Read balance behavior{" "}
                            <i ref={setReadBalanceBehaviorPopover} className="icon-info text-info" />
                            <PopoverWithHover target={readBalanceBehaviorPopover} placement="top">
                                <div className="flex-horizontal p-3">
                                    <div>
                                        Set the load-balance method that the client will use when accessing a node with{" "}
                                        <strong>Read</strong> requests. The method selected will also affect the
                                        client's decision of which node to failover to in case of issues with the{" "}
                                        <strong>Read</strong> request.
                                    </div>
                                </div>
                            </PopoverWithHover>
                        </div>
                    </div>
                    <Row className="mt-1">
                        <Col className="d-flex">
                            <InputGroup>
                                <InputGroupText>
                                    <FormCheckbox
                                        control={control}
                                        name="readBalanceBehaviorEnabled"
                                        disabled={!formValues.overrideConfig}
                                    />
                                </InputGroupText>
                                <FormSelect<ReadBalanceBehavior>
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
