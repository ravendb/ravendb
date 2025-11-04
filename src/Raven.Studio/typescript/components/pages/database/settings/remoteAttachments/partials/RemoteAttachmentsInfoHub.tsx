import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";

export function RemoteAttachmentsInfoHub() {
    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>todo</div>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
