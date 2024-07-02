import React from "react";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";

interface AskCommunityContentProps {
    selectTab: (tab: string) => void;
}

export function AskCommunityContent({ selectTab }: AskCommunityContentProps) {
    const gitHubCommunityUrl = "https://github.com/ravendb/ravendb/discussions";

    return (
        <>
            <ul className="action-menu__list">
                <p className="m-0">
                    Get fast and comprehensive help from fellow RavenDB users and developers in our community forum.
                </p>
                <li
                    className="mt-1 action-menu__list-item action-menu__list-item--primary"
                    role="button"
                    title="Go to GitHub discussions"
                    onClick={() => window.open(gitHubCommunityUrl, "_blank")}
                >
                    <Icon icon="github" margin="m-0" />
                    GitHub Discussions
                    <FlexGrow />
                    <Icon icon="newtab" margin="m-0" />
                </li>
            </ul>
            <div className="action-menu__footer">
                <small className="text-muted">
                    <Icon icon="feedback" />
                    You can also submit feedback{" "}
                    <span
                        role="link"
                        className="text-decoration-underline hover-filter cursor-pointer"
                        onClick={() => selectTab("feedbackForm")}
                    >
                        using form
                    </span>
                </small>
            </div>
        </>
    );
}
