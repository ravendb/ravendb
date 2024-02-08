import React from "react";
import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppUrls } from "components/hooks/useAppUrls";

export default function ConflictResolutionAboutView() {
    const conflictResolutionDocsLink = useRavenLink({ hash: "QRCNKH" });
    const { forCurrentDatabase } = useAppUrls();

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="1"
                icon="about"
                color="info"
                description="Get additional info on this feature"
                heading="About this view"
            >
                <p>
                    Since RavenDB accepts writes on any node in the cluster,
                    <br /> a conflict may occur when the same document is updated concurrently on two different nodes.
                </p>
                <p>
                    In this view, you can <strong>define the server&apos;s behavior</strong> upon a conflict between
                    documents.
                    <br />
                    When a conflict is detected, the server will attempt to resolve it via the following flow:
                </p>
                <ol>
                    <li>
                        If a resolving script (a JavaScript function) is defined per the collection,
                        <br /> it will be used to resolve the conflict.
                        <br /> See the available script variables and examples in the provided documentation link.
                    </li>
                    <li className="margin-top-xs">
                        Else, when no script is defined:
                        <br />
                        <ul>
                            <li className="margin-top-xxs">
                                If the &quot;<strong>Resolve to latest version</strong>&quot; toggle is turned ON:
                                <br />
                                The server will resolve the conflict to the latest document version.
                            </li>
                            <li className="margin-top-xxs">
                                If the toggle is OFF:
                                <br /> The conflict is not automatically resolved.
                                <br /> You can resolve the conflict manually from the{" "}
                                <a href={forCurrentDatabase.conflicts()} target="_blank">
                                    Conflicts view
                                </a>{" "}
                                in the Studio.
                            </li>
                        </ul>
                    </li>
                </ol>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={conflictResolutionDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Conflict Resolution
                </a>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
