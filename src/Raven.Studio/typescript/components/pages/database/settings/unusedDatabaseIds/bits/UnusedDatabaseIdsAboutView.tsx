import { todo } from "common/developmentHelper";
import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";

todo("Other", "Danielle", "About view info");

export default function UnusedDatabaseIdsAboutView() {
    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                description="Get additional info on this feature"
                heading="About this view"
            >
                Text for Unused Database IDs
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
