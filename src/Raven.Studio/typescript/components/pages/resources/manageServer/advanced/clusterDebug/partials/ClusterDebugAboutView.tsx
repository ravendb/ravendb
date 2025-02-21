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
            ></AccordionItemWrapper>
        </AboutViewFloating>
    );
}
