import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";

export default function IndexErrorsAboutView() {
    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    Maintaining multiple indexes can lower performance. Every time data is inserted, updated, or
                    deleted, the corresponding indexes need to be updated as well, which can lead to increased write
                    latency.
                </p>
                <p className="mb-0">
                    To counter these performance issues, RavenDB recommends a set of actions to optimize the number of
                    indexes. Note that you need to update the index reference in your application.
                </p>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
