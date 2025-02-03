import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { useAppSelector } from "components/store";
import { useAppUrls } from "hooks/useAppUrls";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export function RevisionsBinCleanerInfoHub() {
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

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
                    <strong>Automatic deletion</strong>:
                    <br />
                    This view allows you to configure automatic deletion of{" "}
                    <strong>
                        <code>Deleted Revisions</code>
                    </strong>{" "}
                    that are stored in the{" "}
                    <a href={appUrl.forRevisionsBin(activeDatabaseName)} target="_blank">
                        Revisions Bin
                    </a>
                    . You can set the following:
                    <ul className="mt-1">
                        <li className="mb-1">
                            <strong>Minimum age</strong>:
                            <br />
                            Revisions that have been in the Revisions Bin for longer than this duration will be removed
                            when the cleaner executes.
                        </li>
                        <li>
                            <strong>Custom cleaner frequency</strong>:
                            <br />
                            Defines how often the cleaner executes to remove revisions.
                        </li>
                    </ul>
                </p>
                <p>
                    <strong>Manual deletion</strong>:
                    <br />
                    Manual deletion of individual &quot;Deleted Revisions&quot; can be performed from the{" "}
                    <a href={appUrl.forRevisionsBin(activeDatabaseName)} target="_blank">
                        Revisions Bin View
                    </a>
                    .
                </p>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
