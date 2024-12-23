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
            <AccordionItemWrapper
                targetId="1"
                icon="about"
                color="info"
                description="Get additional info on this feature"
                heading="About this view"
            >
                <p>
                    This view displays all revisions for every document in the database.
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
                            <li>A "Delete Revision" is created for a document when the document is deleted.</li>
                            <li>
                                These revisions are also listed in the{" "}
                                <a href={appUrl.forRevisionsBin(activeDatabaseName)} target="_blank">
                                    Revisions Bin
                                </a>{" "}
                                view and can only be deleted from there.
                            </li>
                        </ul>
                    </p>
                </p>
                <p>Exercise caution when deleting revisions, as this action cannot be undone.</p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
