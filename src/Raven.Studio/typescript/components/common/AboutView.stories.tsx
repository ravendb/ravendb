import React from "react";
import { ComponentMeta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import {
    AboutViewFloating,
    AboutViewAnchored,
    AccordionItemWrapper,
    AccordionItemLicensing,
    AboutViewHeading,
    FeatureAvailabilityTable,
    FeatureAvailabilityData,
} from "./AboutView";
import { Button, Col, Row } from "reactstrap";
import { Icon } from "./Icon";
import Code from "./Code";
import AccordionLicenseNotIncluded from "./AccordionLicenseNotIncluded";
import { boundCopy } from "components/utils/common";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Bits/AboutView",
    component: AboutViewFloating,
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        defaultOpen: {
            control: {
                type: "boolean",
            },
        },
        featureAvailable: {
            control: {
                type: "boolean",
            },
        },
    },
} as ComponentMeta<typeof AboutViewFloating>;

const availabilityData: FeatureAvailabilityData[] = [
    {
        featureName: "Future 1",
        community: false,
        enterprise: true,
        professional: true,
    },
    {
        featureName: "Future 2",
        community: "min 36",
        enterprise: Infinity,
        professional: Infinity,
    },
    {
        featureName: "Future 3",
        community: "Yes",
        enterprise: "No",
        professional: "Maybe",
    },
];
const availabilityDataSimple: FeatureAvailabilityData[] = [
    {
        community: false,
        enterprise: true,
        professional: true,
    },
];

const FloatingButton = (args: { defaultOpen: boolean; featureAvailable: boolean }) => {
    const { license } = mockStore;

    license.with_Essential();

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row>
                    <Col>
                        <AboutViewHeading title="Section title" icon="zombie" badgeText="Professional" />
                        default open: {args.defaultOpen ? "true" : "false"} / feature available:{" "}
                        {args.defaultOpen ? "true" : "false"}
                    </Col>
                    <Col sm={"auto"}>
                        <AboutViewFloating defaultOpen={args.defaultOpen}>
                            <AccordionItemWrapper
                                icon="zombie"
                                color="info"
                                heading="About this view"
                                description="haha"
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
                            >
                                <p>
                                    <strong>To set the refresh time:</strong> enter the appropriate date in the metadata{" "}
                                    <code>@refresh</code> property.
                                </p>
                                <p>
                                    <strong>Note:</strong> RavenDB scans which documents should be refreshed at the
                                    frequency specified. The actual refresh time can increase (up to) that value.
                                </p>
                                <Code code={codeExample} language="javascript" />
                            </AccordionItemWrapper>
                            <AccordionItemWrapper
                                icon="license"
                                color={args.featureAvailable ? "success" : "warning"}
                                heading="Licensing"
                                description="Learn how to get the most of this feature"
                                targetId="licensing"
                            >
                                <FeatureAvailabilityTable availabilityData={availabilityData} />
                                <hr />
                                <FeatureAvailabilityTable availabilityData={availabilityDataSimple} />
                            </AccordionItemWrapper>
                            {/* <AccordionLicenseNotIncluded
                                featureName="Document Compression"
                                featureIcon="documents-compression"
                                checkedLicenses={["Professional", "Enterprise"]}
                                isLimited
                            /> */}
                        </AboutViewFloating>
                    </Col>
                </Row>
            </Col>
        </div>
    );
};

const AnchoredHub = (args: { featureAvailable: boolean }) => {
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
                                description="Get additional info on this feature"
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
                            >
                                <p>
                                    <strong>To set the refresh time:</strong> enter the appropriate date in the metadata{" "}
                                    <code>@refresh</code> property.
                                </p>
                                <p>
                                    <strong>Note:</strong> RavenDB scans which documents should be refreshed at the
                                    frequency specified. The actual refresh time can increase (up to) that value.
                                </p>
                                <Code code={codeExample} language="javascript" />
                            </AccordionItemWrapper>
                            <AccordionItemWrapper
                                icon="license"
                                color="warning"
                                heading="Licensing"
                                description="See which plans offer this and more exciting features"
                                pill
                                pillText="Upgrade available"
                                pillIcon="upgrade-arrow"
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

const codeExample = `{
    "Example": "Set a timestamp in the @refresh metadata property",
    "@metadata": {
        "@collection": "Foo",
        "@refresh": "2023-07-16T08:00:00.0000000Z"
    }
}`;

export const Floating = boundCopy(FloatingButton, {
    defaultOpen: true,
    featureAvailable: true,
});

export const Anchored = boundCopy(AnchoredHub, {
    featureAvailable: true,
});
