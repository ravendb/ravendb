import React from "react";
import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";

export default function ClusterDebugAboutView() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>We need some description right here</p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
