import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { todo } from "common/developmentHelper";

todo(
    "Other",
    "Danielle",
    "It is necessary to write a text to this about view",
    "https://issues.hibernatingrhinos.com/issue/RavenDB-21763/Move-server-settings-to-react"
);

export function ServerSettingsInfoHub() {
    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>This is text for Server Settings</p>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
