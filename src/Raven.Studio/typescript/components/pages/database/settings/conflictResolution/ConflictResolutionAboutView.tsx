import React from "react";
import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";

export default function ConflictResolutionAboutView() {
    const conflictResolutionDocsLink = useRavenLink({ hash: "QRCNKH" });

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="1"
                icon="about"
                color="info"
                description="Get additional info on this feature"
                heading="About this view"
            >
                <p>Text for Conflicts Resolution</p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={conflictResolutionDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Conflict Resolution
                </a>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
