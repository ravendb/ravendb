import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import {
    AboutViewFloating,
    AboutViewAnchored,
    AccordionItemWrapper,
    AccordionItemLicensing,
    AboutViewHeading,
} from "./AboutView";
import { Button, Col, Row } from "reactstrap";
import { Icon } from "./Icon";

export default {
    title: "Bits/AboutView",
    component: AboutViewFloating,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof AboutViewFloating>;

export const FloatingButton: ComponentStory<typeof AboutViewFloating> = () => {
    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row>
                    <Col>
                        <AboutViewHeading title="Section title" icon="zombie" badge badgeText="Professional" />
                    </Col>
                    <Col sm={"auto"}>
                        <AboutViewFloating>
                            <AccordionItemWrapper
                                icon="zombie"
                                color="info"
                                heading="About this view"
                                description="haha"
                                targetId="1"
                            >
                                <Col>
                                    <p>
                                        <strong>Admin JS Console</strong> is a specialized feature primarily intended
                                        for resolving server errors. It provides a direct interface to the underlying
                                        system, granting the capacity to execute scripts for intricate server
                                        operations.
                                    </p>
                                    <p>
                                        It is predominantly intended for advanced troubleshooting and rectification
                                        procedures executed by system administrators or RavenDB support.
                                    </p>
                                    <hr />
                                    <div className="small-label mb-2">useful links</div>
                                    <a href="https://ravendb.net/l/IBUJ7M/6.0/Csharp" target="_blank">
                                        <Icon icon="newtab" /> Docs - Admin JS Console
                                    </a>
                                </Col>
                            </AccordionItemWrapper>
                            <AccordionItemWrapper
                                icon="road-cone"
                                color="success"
                                heading="Examples of use"
                                description="Learn how to get the most of this feature"
                                targetId="2"
                            >
                                <p>
                                    <strong>To set the refresh time:</strong> enter the appropriate date in the metadata{" "}
                                    <code>@refresh</code> property.
                                </p>
                                <p>
                                    <strong>Note:</strong> RavenDB scans which documents should be refreshed at the
                                    frequency specified. The actual refresh time can increase (up to) that value.
                                </p>
                                <pre className="bg-black rounded-3 p-3">
                                    <code className="language-javascript">
                                        {`function greet(name) {
    console.log('Hello, ' + name + '!');
}`}
                                    </code>
                                </pre>
                            </AccordionItemWrapper>
                            <AccordionItemWrapper
                                icon="license"
                                color="warning"
                                heading="Licensing"
                                description="See which plans offer this and more exciting features"
                                targetId="3"
                                pill
                                pillText="Upgrade available"
                                pillIcon="star-filled"
                            >
                                <AccordionItemLicensing
                                    description="This feature is not available in your license. Unleash the full potential and upgrade your plan."
                                    featureName="Document Compression"
                                    featureIcon="documents-compression"
                                    checkedLicenses={["Professional", "Enterprise"]}
                                >
                                    <p className="lead fs-4">Get your license expanded</p>
                                    <div className="mb-3">
                                        <Button color="primary" className="rounded-pill">
                                            <Icon icon="notifications" />
                                            Contact us
                                        </Button>
                                    </div>
                                    <small>
                                        <a href="#" target="_blank" className="text-muted">
                                            See pricing plans
                                        </a>
                                    </small>
                                </AccordionItemLicensing>
                            </AccordionItemWrapper>
                        </AboutViewFloating>
                    </Col>
                </Row>
            </Col>
        </div>
    );
};

export const AnchoredHub: ComponentStory<typeof AboutViewAnchored> = () => {
    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Section title" icon="zombie" />
                        <div className="bg-dark w-100 h-100"></div>
                    </Col>
                    <Col sm={12} md={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper
                                icon="about"
                                color="info"
                                heading="About this view"
                                description="Get additional info on what this feature can offer you"
                                targetId="1"
                            >
                                <p>
                                    <strong>Admin JS Console</strong> is a specialized feature primarily intended for
                                    resolving server errors. It provides a direct interface to the underlying system,
                                    granting the capacity to execute scripts for intricate server operations.
                                </p>
                                <p>
                                    It is predominantly intended for advanced troubleshooting and rectification
                                    procedures executed by system administrators or RavenDB support.
                                </p>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href="https://ravendb.net/l/IBUJ7M/6.0/Csharp" target="_blank">
                                    <Icon icon="newtab" /> Docs - Admin JS Console
                                </a>
                            </AccordionItemWrapper>
                            <AccordionItemWrapper
                                icon="road-cone"
                                color="success"
                                heading="Examples of use"
                                description="Learn how to get the most of this feature"
                                targetId="2"
                            >
                                <p>
                                    <strong>To set the refresh time:</strong> enter the appropriate date in the metadata{" "}
                                    <code>@refresh</code> property.
                                </p>
                                <p>
                                    <strong>Note:</strong> RavenDB scans which documents should be refreshed at the
                                    frequency specified. The actual refresh time can increase (up to) that value.
                                </p>
                                <pre className="bg-black rounded-3 p-3">
                                    <code className="language-javascript">
                                        {`function greet(name) {
    console.log('Hello, ' + name + '!');
}`}
                                    </code>
                                </pre>
                            </AccordionItemWrapper>
                            <AccordionItemWrapper
                                icon="license"
                                color="warning"
                                heading="Licensing"
                                description="See which plans offer this and more exciting features"
                                targetId="3"
                                pill
                                pillText="Upgrade available"
                                pillIcon="star-filled"
                            >
                                <AccordionItemLicensing
                                    description="This feature is not available in your license. Unleash the full potential and upgrade your plan."
                                    featureName="Document Compression"
                                    featureIcon="documents-compression"
                                    checkedLicenses={["Professional", "Enterprise"]}
                                >
                                    <p className="lead fs-4">Get your license expanded</p>
                                    <div className="mb-3">
                                        <Button color="primary" className="rounded-pill">
                                            <Icon icon="notifications" />
                                            Contact us
                                        </Button>
                                    </div>
                                    <small>
                                        <a href="#" target="_blank" className="text-muted">
                                            See pricing plans
                                        </a>
                                    </small>
                                </AccordionItemLicensing>
                            </AccordionItemWrapper>
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
};
