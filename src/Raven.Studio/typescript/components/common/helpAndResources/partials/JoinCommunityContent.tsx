import React from "react";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import { useRavenLink } from "components/hooks/useRavenLink";
import { VStack } from "components/common/utilities/VStack";

interface JoinCommunityContentProps {
    openFeedbackForm: () => void;
}

export function JoinCommunityContent({ openFeedbackForm }: JoinCommunityContentProps) {
    const gitHubDiscussionsUrl = useRavenLink({ hash: "ITXUEA" });
    const discordServerUrl = useRavenLink({ hash: "ZL8MVM" });

    return (
        <>
            <ul className="action-menu__list">
                <p className="m-0">
                    Get advice and share ideas on our Developers Community Discord. For business-specific technical
                    help, visit GitHub Discussions.
                </p>
                <VStack gap={1} className="mt-1">
                    <li
                        className="action-menu__list-item action-menu__list-item--primary"
                        role="button"
                        title="Go to Developers Community Discord"
                        onClick={() => window.open(discordServerUrl, "_blank")}
                    >
                        <Icon icon="discord" margin="m-0" />
                        Discord Community
                        <FlexGrow />
                        <Icon icon="newtab" margin="m-0" />
                    </li>
                    <li
                        className="action-menu__list-item action-menu__list-item--primary"
                        role="button"
                        title="Go to GitHub discussions"
                        onClick={() => window.open(gitHubDiscussionsUrl, "_blank")}
                    >
                        <Icon icon="github" margin="m-0" />
                        GitHub Discussions
                        <FlexGrow />
                        <Icon icon="newtab" margin="m-0" />
                    </li>
                </VStack>
            </ul>
            <div className="action-menu__footer">
                <small>
                    <Icon icon="feedback" />
                    You can also submit feedback{" "}
                    <span
                        role="link"
                        className="text-decoration-underline hover-filter cursor-pointer m-0"
                        onClick={openFeedbackForm}
                    >
                        using form
                    </span>
                </small>
            </div>
        </>
    );
}
