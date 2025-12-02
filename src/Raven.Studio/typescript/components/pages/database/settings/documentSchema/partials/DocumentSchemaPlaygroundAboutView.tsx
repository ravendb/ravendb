import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";

export default function DocumentSchemaPlaygroundAboutView() {
    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                icon="about"
                color="info"
                description="Get additional info on this feature"
                heading="About this view"
            >
                todo
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
