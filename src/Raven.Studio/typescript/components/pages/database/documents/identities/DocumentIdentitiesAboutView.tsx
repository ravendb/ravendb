import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { todo } from "common/developmentHelper";

todo(
    "Other",
    "Danielle",
    "It is necessary to write a text to this about view.",
    "https://issues.hibernatingrhinos.com/issue/RavenDB-21764/Move-identities-to-react"
);

export default function DocumentIdentitiesAboutView() {
    return (
        <AboutViewAnchored className="my-4">
            <AccordionItemWrapper
                targetId="1"
                icon="about"
                color="info"
                description="Get additional info on what this feature can offer you"
                heading="About this view"
            >
                <div></div>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
