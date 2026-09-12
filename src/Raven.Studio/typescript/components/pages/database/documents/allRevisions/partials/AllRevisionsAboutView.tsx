import React from "react";
import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import { useAppUrls } from "hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export default function AllRevisionsAboutView() {
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="1" icon="about" color="info">
                <p>
                    This view displays ALL revisions for every document in the database.
                    <br />
                    You can filter the revisions by their <strong>Collection</strong> or <strong>Type</strong>:
                    <p className="mt-3">
                        <strong>Regular revisions</strong>:
                        <ul>
                            <li>A regular revision is created for a document whenever it is modified.</li>
                            <li>
                                These revisions can be deleted directly from this view.
                                <br /> Once a revision is removed, it will no longer be available in the database and
                                will Not be listed in this view.
                            </li>
                        </ul>
                    </p>
                    <p>
                        <strong>Deleted revisions</strong>:
                        <ul>
                            <li>
                                A &quot;Delete Revision&quot; is created for a document when the document is deleted.
                                These revisions are also listed in the Revisions Bin View.
                            </li>
                            <li>
                                &quot;Delete revisions&quot; can be deleted from:
                                <ul className="no-padding-left">
                                    <li>
                                        The{" "}
                                        <a href={appUrl.forRevisionsBin(activeDatabaseName)} target="_blank">
                                            Revisions Bin View
                                        </a>{" "}
                                    </li>
                                    <li>
                                        Or set automatic deletion using the{" "}
                                        <a href={appUrl.forRevisionsBinCleaner(activeDatabaseName)} target="_blank">
                                            Revisions Bin Cleaner
                                        </a>
                                    </li>
                                </ul>
                            </li>
                        </ul>
                    </p>
                </p>
                <p>Exercise caution when deleting revisions, as this action cannot be undone.</p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
