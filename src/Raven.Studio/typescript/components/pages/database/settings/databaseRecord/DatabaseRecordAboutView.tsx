import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import React from "react";

export default function DatabaseRecordAboutView() {
    const databaseRecordDocsLink = useRavenLink({ hash: "LJNTFQ" });

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="1"
                icon="about"
                color="info"
                description="Get additional info on this feature"
                heading="About this view"
            >
                <ul>
                    <li>
                        The Database Record stores all settings and configurations for the database and is consistently
                        updated. Each node in the cluster maintains an up-to-date copy of this document.
                    </li>
                    <li className={"margin-top-sm"}>
                        Editing the Database Record is strongly discouraged.
                        <br />
                        Changes may lead to unintended consequences, including the loss of the entire database and its
                        data.
                    </li>
                </ul>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={databaseRecordDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Database Record
                </a>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
