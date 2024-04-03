import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";

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
                <ul>
                    <li className="margin-top-xm">
                        Each database instance in the cluster has its own database ID (listed under &quot;
                        <strong>Used IDs</strong>&quot;).
                        <br /> These IDs are used to construct the change-vector generated for each document that is
                        created or modified within the database.
                    </li>
                    <li className="margin-top-xs">
                        In some scenarios, such as restoring a database from a Snapshot, or when receiving data from a
                        source database via an External-Replication task, the change-vectors of incoming documents,
                        which include the source database IDs, are added to the change-vector of the target database.
                    </li>
                    <li className="margin-top-xs">
                        To avoid using these database IDs which are no longer relevant in the current database,
                        <br /> add them to the list of &quot;<strong>Unused IDs</strong>&quot; so they will not be
                        utilized when creating new change-vectors.
                    </li>
                    <li className="margin-top-xs">
                        Note:
                        <br /> Do not add database IDs that are currently in use by other nodes in the database-group to
                        this list.
                    </li>
                </ul>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
