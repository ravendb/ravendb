import React, { useEffect } from "react";
import { Form, Col, Button, Card, Row, Spinner, InputGroup, UncontrolledPopover } from "reactstrap";
import { SubmitHandler, useForm } from "react-hook-form";
import { FormCheckbox, FormInput, FormSelect, FormSwitch } from "components/common/Form";
import {
    ClientConfigurationFormData,
    clientConfigurationYupResolver,
} from "../../../../common/clientConfiguration/ClientConfigurationValidation";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";
import useClientConfigurationFormController from "components/common/clientConfiguration/useClientConfigurationFormController";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import FeatureNotAvailableInYourLicensePopover from "components/common/FeatureNotAvailableInYourLicensePopover";

export default function ClientGlobalConfiguration() {
    const { manageServerService } = useServices();
    const asyncGetGlobalClientConfiguration = useAsyncCallback(manageServerService.getGlobalClientConfiguration);

    const { handleSubmit, control, formState, setValue, reset } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "all",
        defaultValues: async () =>
            ClientConfigurationUtils.mapToFormData(await asyncGetGlobalClientConfiguration.execute(), true),
    });

    const loadBalancingDocsLink = useRavenLink({ hash: "GYJ8JA" });
    const clientConfigurationDocsLink = useRavenLink({ hash: "TS7SGF" });

    const formValues = useClientConfigurationFormController(control, setValue, true);

    useEffect(() => {
        if (formState.isSubmitSuccessful) {
            reset(formValues);
        }
    }, [formState.isSubmitSuccessful, reset, formValues]);

    useDirtyFlag(formState.isDirty);

    const hasClientConfiguration = useAppSelector(licenseSelectors.statusValue("HasClientConfiguration"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasClientConfiguration,
            },
        ],
    });

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveGlobalClientConfiguration(ClientConfigurationUtils.mapToDto(formData, true));
        });
    };

    const onRefresh = async () => {
        reset(ClientConfigurationUtils.mapToFormData(await asyncGetGlobalClientConfiguration.execute(), true));
    };

    if (asyncGetGlobalClientConfiguration.loading) {
        return <LoadingView />;
    }

    if (asyncGetGlobalClientConfiguration.error) {
        return <LoadError error="Unable to load client global configuration" refresh={onRefresh} />;
    }

    return (
        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
            <div className="content-margin">
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading
                            icon="database-client-configuration"
                            title="Server-Wide Client Configuration"
                            licenseBadgeText={hasClientConfiguration ? null : "Professional +"}
                        />
                        <div id="saveClientConfiguration" className="w-fit-content mb-3">
                            <Button
                                type="submit"
                                color="primary"
                                disabled={formState.isSubmitting || !formState.isDirty}
                            >
                                {formState.isSubmitting ? <Spinner size="sm" className="me-1" /> : <Icon icon="save" />}
                                Save
                            </Button>
                        </div>
                        {!hasClientConfiguration && (
                            <FeatureNotAvailableInYourLicensePopover target="saveClientConfiguration" />
                        )}
                        <div className={hasClientConfiguration ? "" : "item-disabled pe-none"}>
                            <Card className="flex-column p-4">
                                <div>
                                    <div className="d-flex flex-grow-1">
                                        <div className="md-label">
                                            Identity parts separator{" "}
                                            <Icon id="SetIdentityPartsSeparator" icon="info" color="info" />
                                        </div>
                                        <UncontrolledPopover
                                            target="SetIdentityPartsSeparator"
                                            trigger="hover"
                                            container="PopoverContainer"
                                            placement="right"
                                        >
                                            <div className="p-3">
                                                Set the default separator for automatically generated document identity
                                                IDs.
                                                <br />
                                                Use any character except <code>&apos;|&apos;</code> (pipe).
                                            </div>
                                        </UncontrolledPopover>
                                    </div>
                                    <Row>
                                        <Col>
                                            <InputGroup>
                                                <div className="toggle-field-checkbox">
                                                    <FormCheckbox
                                                        control={control}
                                                        name="identityPartsSeparatorEnabled"
                                                    />
                                                </div>
                                                <FormInput
                                                    type="text"
                                                    control={control}
                                                    name="identityPartsSeparatorValue"
                                                    placeholder="'/' (default)"
                                                    disabled={!formValues.identityPartsSeparatorEnabled}
                                                    className="d-flex"
                                                />
                                            </InputGroup>
                                        </Col>
                                    </Row>
                                </div>
                                <div className="mt-4">
                                    <div className="d-flex flex-grow-1">
                                        <div className="md-label">
                                            Maximum number of requests per session{" "}
                                            <Icon id="SetMaximumRequestsPerSession" icon="info" color="info" />
                                        </div>
                                        <UncontrolledPopover
                                            target="SetMaximumRequestsPerSession"
                                            trigger="hover"
                                            container="PopoverContainer"
                                            placement="right"
                                        >
                                            <div className="p-3">
                                                Set this number to restrict the number of requests (<code>Reads</code> &{" "}
                                                <code>Writes</code>) per session in the client API.
                                            </div>
                                        </UncontrolledPopover>
                                    </div>
                                    <Row>
                                        <Col>
                                            <InputGroup>
                                                <div className="toggle-field-checkbox">
                                                    <FormCheckbox
                                                        control={control}
                                                        name="maximumNumberOfRequestsEnabled"
                                                    />
                                                </div>
                                                <FormInput
                                                    type="number"
                                                    control={control}
                                                    name="maximumNumberOfRequestsValue"
                                                    placeholder="30 (default)"
                                                    disabled={!formValues.maximumNumberOfRequestsEnabled}
                                                />
                                            </InputGroup>
                                        </Col>
                                    </Row>
                                </div>
                            </Card>
                            <div className="d-flex justify-content-between mt-4 position-relative">
                                <h5>Load Balancing Client Requests</h5>
                                <small title="Navigate to the documentation" className="position-absolute end-0">
                                    <a href={loadBalancingDocsLink} target="_blank">
                                        <Icon icon="link" /> Load balancing tutorial
                                    </a>
                                </small>
                            </div>
                            <Card className="flex-column p-4">
                                <div className="d-flex flex-grow-1">
                                    <div className="md-label">
                                        Load Balance Behavior <Icon id="SetSessionContext" icon="info" color="info" />
                                        <UncontrolledPopover
                                            target="SetSessionContext"
                                            trigger="hover"
                                            container="PopoverContainer"
                                            placement="right"
                                        >
                                            <div className="p-3">
                                                <span className="d-inline-block mb-1">
                                                    Set the Load balance method for <strong>Read</strong> &{" "}
                                                    <strong>Write</strong> requests.
                                                </span>
                                                <ul>
                                                    <li className="mb-1">
                                                        <code>None</code>
                                                        <br />
                                                        <strong>Read</strong> requests - the node the client will target
                                                        will be based on Read balance behavior configuration.
                                                        <br />
                                                        <strong>Write</strong> requests - will be sent to the preferred
                                                        node.
                                                    </li>
                                                    <li className="mb-1">
                                                        <code>Use session context</code>
                                                        <br />
                                                        Sessions that are assigned the same context will have all their{" "}
                                                        <strong>Read & Write</strong> requests routed to the same node.
                                                        <br />
                                                        The session context is hashed from a context string (given by
                                                        the client) and an optional seed.
                                                    </li>
                                                </ul>
                                            </div>
                                        </UncontrolledPopover>
                                    </div>
                                </div>
                                <Row>
                                    <Col>
                                        <InputGroup>
                                            <div className="toggle-field-checkbox">
                                                <FormCheckbox control={control} name="loadBalancerEnabled" />
                                            </div>
                                            <FormSelect
                                                control={control}
                                                name="loadBalancerValue"
                                                isDisabled={!formValues.loadBalancerEnabled}
                                                options={ClientConfigurationUtils.getLoadBalanceBehaviorOptions()}
                                                isSearchable={false}
                                            />
                                        </InputGroup>
                                    </Col>
                                </Row>
                                {formValues.loadBalancerValue === "UseSessionContext" && (
                                    <>
                                        <div className="md-label mt-4">
                                            Seed
                                            <Icon id="SetLoadBalanceSeedBehavior" icon="info" color="info" />
                                            <UncontrolledPopover
                                                target="SetLoadBalanceSeedBehavior"
                                                trigger="hover"
                                                container="PopoverContainer"
                                                placement="right"
                                            >
                                                <div className="p-3">
                                                    An optional seed number.
                                                    <br />
                                                    Used when hashing the session context.
                                                </div>
                                            </UncontrolledPopover>
                                        </div>
                                        <Row>
                                            <Col className="hstack gap-3">
                                                <FormSwitch
                                                    control={control}
                                                    name="loadBalancerSeedEnabled"
                                                    color="primary"
                                                    label="Seed"
                                                    className="small"
                                                />
                                                <InputGroup>
                                                    <FormInput
                                                        type="number"
                                                        control={control}
                                                        name="loadBalancerSeedValue"
                                                        placeholder="0 (default)"
                                                        disabled={!formValues.loadBalancerSeedEnabled}
                                                    />
                                                </InputGroup>
                                            </Col>
                                        </Row>
                                    </>
                                )}
                                <div className="d-flex flex-grow-1">
                                    <div className="md-label mt-4">
                                        Read Balance Behavior{" "}
                                        <Icon id="SetReadBalanceBehavior" icon="info" color="info" />
                                        <UncontrolledPopover
                                            target="SetReadBalanceBehavior"
                                            trigger="hover"
                                            container="PopoverContainer"
                                            placement="right"
                                        >
                                            <div className="p-3">
                                                <div>
                                                    Set the Read balance method the client will use when accessing a
                                                    node with <code>Read</code> requests.
                                                    <br />
                                                    <code>Write</code> requests are sent to the preferred node.
                                                </div>
                                            </div>
                                        </UncontrolledPopover>
                                    </div>
                                </div>
                                <Row>
                                    <Col>
                                        <InputGroup>
                                            <div className="toggle-field-checkbox">
                                                <FormCheckbox control={control} name="readBalanceBehaviorEnabled" />
                                            </div>
                                            <FormSelect
                                                control={control}
                                                name="readBalanceBehaviorValue"
                                                isDisabled={!formValues.readBalanceBehaviorEnabled}
                                                options={ClientConfigurationUtils.getReadBalanceBehaviorOptions()}
                                                isSearchable={false}
                                            />
                                        </InputGroup>
                                    </Col>
                                </Row>
                            </Card>
                        </div>
                    </Col>
                    <Col sm={12} md={4}>
                        <AboutViewAnchored defaultOpen={hasClientConfiguration ? null : "licensing"}>
                            <AccordionItemWrapper
                                icon="about"
                                color="info"
                                heading="About this view"
                                description="Get additional info on this feature"
                                targetId="1"
                            >
                                <ul>
                                    <li className="margin-bottom-xs">
                                        This is the <strong>Server-wide Client-Configuration</strong> view.
                                        <br />
                                        The available Client-Configuration options will apply to any client that
                                        communicates with any database in the cluster.
                                    </li>
                                    <li>
                                        These values can be customized per database in the{" "}
                                        <strong>Database Client-Configuration</strong> view.
                                    </li>
                                </ul>
                                <hr />
                                <ul>
                                    <li className="margin-bottom-xs">
                                        Setting the Client-Configuration on the server from this view will{" "}
                                        <strong>override</strong> the client&apos;s existing settings, which were
                                        initially set by your client code.
                                    </li>
                                    <li className="margin-bottom-xs">
                                        When the server&apos;s Client-Configuration is modified, the running client will
                                        receive the updated settings the next time it makes a request to the server.
                                    </li>
                                    <li>
                                        This enables administrators to{" "}
                                        <strong>dynamically control the client behavior</strong> even after it has
                                        started running. E.g. manage load balancing of client requests on the fly in
                                        response to changing system demands.
                                    </li>
                                </ul>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href={clientConfigurationDocsLink} target="_blank">
                                    <Icon icon="newtab" /> Docs - Client Configuration
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={hasClientConfiguration}
                                data={featureAvailability}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </div>
            <div id="PopoverContainer"></div>
        </Form>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Client Configuration",
        featureIcon: "client-configuration",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
