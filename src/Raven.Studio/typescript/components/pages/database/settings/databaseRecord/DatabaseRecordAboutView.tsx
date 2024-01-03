import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import React from "react";

export default function DatabaseRecordAboutView() {
    const databaseRecordDocsLink = useRavenLink({ hash: "QRCNKH" });

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="1"
                icon="about"
                color="info"
                description="Get additional info on this feature"
                heading="About this view"
            >
                <p>Text for Database Record</p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={databaseRecordDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Database Record
                </a>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
