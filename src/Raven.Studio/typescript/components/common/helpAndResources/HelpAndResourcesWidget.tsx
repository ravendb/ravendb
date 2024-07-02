import React, { useState } from "react";
import { Icon } from "components/common/Icon";
import "./HelpAndResources.scss";
import { FlexGrow } from "components/common/FlexGrow";
import { UncontrolledTooltip } from "reactstrap";
import GitHubButton from "react-github-btn";
import { AskCommunityContent } from "components/common/helpAndResources/partials/AskCommunityContent";
import { FeedbackFormContent } from "components/common/helpAndResources/partials/FeedbackFormContent";
import { ContactSupportContent } from "components/common/helpAndResources/partials/ContactSupportContent";
import { useAboutPage } from "components/pages/resources/about/useAboutPage";
import { todo } from "common/developmentHelper";

interface HelpAndResourcesWidgetProps {}

export function HelpAndResourcesWidget(props: HelpAndResourcesWidgetProps) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const [selectedTab, setSelectedTab] = useState<string | null>(null);

    const { asyncCheckLicenseServerConnectivity } = useAboutPage();

    const toggleMenu = () => {
        setIsMenuOpen(!isMenuOpen);
        setSelectedTab("default");
    };

    const selectTab = (tab: string) => {
        setSelectedTab(tab);
    };

    const renderHeader = () => {
        switch (selectedTab) {
            case "askCommunity":
                return "Ask the community";
            case "contactSupport":
                return "Contact support";
            case "feedbackForm":
                return "Submit feedback";
            default:
                return "Help and resources";
        }
    };

    todo("Feature", "Damian", "Connect widget to the shell");

    return (
        <section className="bottom-right-tools-container">
            {!isMenuOpen && (
                <>
                    <span role="button" id="helpWidget" className="btn-help-widget" onClick={toggleMenu}>
                        <Icon icon="question" margin="m-0" />
                    </span>
                    <UncontrolledTooltip target="helpWidget">Help and resources</UncontrolledTooltip>
                </>
            )}
            {isMenuOpen && (
                <div className="action-menu">
                    <span role="button" className="btn-hide-widget" onClick={toggleMenu} title="Hide widget">
                        <Icon icon="close" margin="m-0" className="lh-base" />
                    </span>
                    <div className="action-menu__header gap-2">
                        {selectedTab !== "default" && (
                            <span
                                role="button"
                                className="action-menu__back"
                                onClick={() => selectTab("default")}
                                title="Back to Help and resources"
                            >
                                <Icon icon="arrow-thin-left" margin="m-0" />
                            </span>
                        )}
                        <h4 className="lh-base">{renderHeader()}</h4>
                    </div>
                    {selectedTab === "default" && (
                        <>
                            <ul className="action-menu__list">
                                <li
                                    className="action-menu__list-item action-menu__list-item--highlight"
                                    role="button"
                                    title="Ask the community"
                                    onClick={() => selectTab("askCommunity")}
                                >
                                    <Icon icon="group" margin="m-0" />
                                    Ask the community
                                </li>
                                <li
                                    className="action-menu__list-item"
                                    role="button"
                                    title="Contact support"
                                    onClick={() => selectTab("contactSupport")}
                                >
                                    <Icon icon="support" margin="m-0" />
                                    Contact support
                                </li>
                                <li className="action-menu__list-item" role="button" title="See documentation">
                                    <Icon icon="document2" margin="m-0" />
                                    See documentation
                                    <FlexGrow />
                                    <Icon icon="newtab" margin="m-0" />
                                </li>
                            </ul>
                            <div className="action-menu__footer">
                                <div className="d-flex flex-row align-items-center">
                                    <small className="text-muted lh-1 mb-1">See our project on GitHub</small>
                                    <FlexGrow />
                                    <GitHubButton
                                        href="https://github.com/ravendb/ravendb"
                                        data-color-scheme="no-preference: dark; light: light; dark: dark;"
                                        data-size="large"
                                        data-show-count="true"
                                        aria-label="Star ravendb/ravendb on GitHub"
                                    >
                                        Star
                                    </GitHubButton>
                                </div>
                            </div>
                        </>
                    )}
                    {selectedTab === "askCommunity" && <AskCommunityContent selectTab={selectTab} />}
                    {selectedTab === "feedbackForm" && <FeedbackFormContent />}
                    {selectedTab === "contactSupport" && (
                        <ContactSupportContent
                            asyncCheckLicenseServerConnectivity={asyncCheckLicenseServerConnectivity}
                        />
                    )}
                </div>
            )}
        </section>
    );
}
