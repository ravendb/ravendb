import React from "react";
import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";

export default function AllRevisionsAboutView() {
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
                    <ul className={"margin-top-xs"}>
                        <li>
                            <strong>Regular revisions</strong>:
                            <br />A regular revision is created for a document whenever it is modified.
                        </li>
                        <li className={"margin-top-xs"}>
                            <strong>Deleted revisions</strong>:
                            <br />A deleted revision is created for a document when the document is deleted. These
                            revisions are also listed in the Revisions Bin view.
                        </li>
                    </ul>
                </p>
                <p>
                    From this view, you can delete selected revisions. Once a revision is removed, it will no longer be
                    available in the database and will Not be listed in this view.
                </p>
                <p>Exercise caution when deleting revisions, as this action cannot be undone.</p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
