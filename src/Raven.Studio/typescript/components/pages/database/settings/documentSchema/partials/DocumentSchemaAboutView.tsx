import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";

export default function DocumentSchemaAboutView() {
    const jsonSchemaLink = "https://json-schema.org";

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                icon="about"
                color="info"
                description="Get additional info on this feature"
                heading="About this view"
            >
                <div>
                    <ul>
                        <li>
                            Use this view to define a <strong>JSON Schema per collection</strong> using the standard{" "}
                            <a href={jsonSchemaLink} target="_blank">
                                JSON Schema format
                            </a>
                            .
                        </li>
                        <li className="mt-2">
                            Document validation according to the defined schema is <strong>enforced</strong> in the
                            following scenarios:
                            <ul>
                                <li>
                                    Inserting documents via <i>session</i> & via <i>bulk insert</i> - invalid documents
                                    are rejected.
                                </li>
                                <li>
                                    Inserting documents via <i>patch by query</i> - the operation stops when an invalid
                                    document is encountered.
                                </li>
                                <li>Reverting revisions - invalid documents produced by the revert are rejected.</li>
                                <li>Importing database - import stops when an invalid document is encountered.</li>
                                <li>
                                    RavenDB ETL task - If the task generates documents that are invalid for the
                                    destination database, the task is automatically paused until the issue is resolved.
                                </li>
                            </ul>
                        </li>
                        <li className="mt-2">
                            Validation is <strong>bypassed</strong> (skipped) for the following scenarios:
                            <ul>
                                <li>Internal replication</li>
                                <li>External replication</li>
                                <li>Restoring a database from backup</li>
                            </ul>
                        </li>
                        <li className="mt-2">
                            RavenDB validates most standard JSON Schema keywords, <strong>except</strong>:
                            <ul>
                                <li>
                                    The <code>unevaluatedProperties</code> keyword is not validated.
                                </li>
                                <li>Recursive references are not supported.</li>
                            </ul>
                        </li>
                        <li className="mt-2">
                            Once defined, you can enable or disable schema validation per collection from this view.
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
