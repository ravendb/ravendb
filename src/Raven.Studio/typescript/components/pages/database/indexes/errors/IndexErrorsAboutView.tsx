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
                    An indexing error can occur during indexing if the indexing function is malformed or if the document
                    data is corrupted or missing. When the error rate exceeds a certain threshold, the index state is
                    marked as <i>Error</i>, and queries cannot be made to it.
                </p>
                <p>This view lists the current index errors for all indexes across all cluster nodes:</p>
                <ul>
                    <li>
                        <strong>Filter viewed indexes</strong>
                        <br />
                        You can filter the list of viewed indexes by specific indexes or by action.
                        <br />
                        The action refers to the part of the indexing process where the error occurred, such as{" "}
                        <i>map</i>,<i>reduce</i>, or <i>analyzer</i>.
                    </li>
                    <li className="mt-1">
                        <strong>Delete errors</strong>
                        <br />
                        You can delete errors for selected indexes.
                        <br />
                        Note that deleting errors only removes the errors; the index state will not change. It will
                        remain in an <b>Error</b> state and will not be set back to the <b>Normal</b> state.
                    </li>
                </ul>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
