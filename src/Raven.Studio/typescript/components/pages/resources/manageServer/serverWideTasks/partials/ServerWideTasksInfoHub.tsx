import React from "react";
import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";

export function ServerWideTasksInfoHub() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
                targetId="about"
            >
                <p>
                    <strong>Server-Wide Tasks</strong> are ongoing tasks defined once at the cluster level and applied
                    to all databases in the cluster.
                </p>
                <ul>
                    <li>
                        <strong>Server-wide Backup</strong> creates periodic backups or snapshots of all databases in
                        your cluster.
                    </li>
                    <li>
                        <strong>Server-wide External Replication</strong> creates live replicas of all databases in
                        another cluster.
                    </li>
                </ul>
                <p>Specific databases can be excluded from each task.</p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
