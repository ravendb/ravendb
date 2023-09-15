import React from "react";
import { ComponentMeta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import { AboutViewFloating, AboutViewAnchored, AccordionItemWrapper, AboutViewHeading } from "./AboutView";
import { Col, Row } from "reactstrap";
import { Icon } from "./Icon";
import Code from "./Code";
import { boundCopy } from "components/utils/common";
import { mockStore } from "test/mocks/store/MockStore";
import { FeatureAvailabilityData, FeatureAvailabilitySummary } from "./FeatureAvailabilitySummary";

export default {
    title: "Bits/AboutView",
    component: AboutViewFloating,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof AboutViewFloating>;

const availabilityData: FeatureAvailabilityData[] = [
    {
        featureName: "Future 1",
        community: { value: false },
        enterprise: { value: true },
        professional: { value: true },
    },
    {
        featureName: "Future 2",
        community: { value: "min 36" },
        enterprise: { value: Infinity },
        professional: { value: Infinity },
    },
    {
        featureName: "Future 3",
        community: { value: "Yes" },
        enterprise: { value: "No" },
        professional: { value: "Maybe" },
    },
];
const availabilityDataSimple: FeatureAvailabilityData[] = [
    {
        community: { value: false },
        enterprise: { value: true },
        professional: { value: true },
    },
];

const FloatingButton = (args: { defaultOpen: boolean; featureAvailable: boolean }) => {
    const { license } = mockStore;

    license.with_License();

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
                        <AboutViewFloating defaultOpen={args.defaultOpen ? "licensing" : null}>
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
                                <FeatureAvailabilitySummary data={availabilityData} />
                            </AccordionItemWrapper>
                        </AboutViewFloating>
                    </Col>
                </Row>
            </Col>
        </div>
    );
};

const AnchoredHub = (args: { featureAvailable: boolean }) => {
    const { license } = mockStore;
    license.with_License({ Type: "Community" });

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
                                color={args.featureAvailable ? "success" : "warning"}
                                heading="Licensing"
                                description="Learn how to get the most of this feature"
                                targetId="licensing"
                            >
                                <FeatureAvailabilitySummary data={availabilityDataSimple} />
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
