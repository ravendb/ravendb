import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";

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
