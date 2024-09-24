import React, { useState } from "react";
import { Icon } from "components/common/Icon";
import "./HelpAndResources.scss";
import { FlexGrow } from "components/common/FlexGrow";
import { UncontrolledTooltip } from "reactstrap";
import { AskCommunityContent } from "components/common/helpAndResources/partials/AskCommunityContent";
import { FeedbackFormContent } from "components/common/helpAndResources/partials/FeedbackFormContent";
import { ContactSupportContent } from "components/common/helpAndResources/partials/ContactSupportContent";
import useBoolean from "components/hooks/useBoolean";
import GithubButtonWithStars from "components/common/helpAndResources/partials/GithubButtonWithStars";
import SeeDocumentationButton from "components/common/helpAndResources/partials/SeeDocumentationButton";

type Tab = "Help and resources" | "Ask the community" | "Contact support" | "Submit feedback";

export function HelpAndResourcesWidget() {
    const { value: isMenuOpen, setValue: setIsMenuOpen } = useBoolean(false);
    const [activeTab, setActiveTab] = useState<Tab>("Help and resources");

    const toggleIsMenuOpen = () => {
        setIsMenuOpen(!isMenuOpen);
        if (isMenuOpen) {
            setActiveTab("Help and resources");
        }
    };

    return (
        <section className="bottom-right-tools-container">
            {!isMenuOpen && (
                <>
                    <span role="button" id="helpWidget" className="btn-help-widget" onClick={toggleIsMenuOpen}>
                        <Icon icon="question" margin="m-0" />
                    </span>
                    <UncontrolledTooltip target="helpWidget">Help and resources</UncontrolledTooltip>
                </>
            )}
            {isMenuOpen && (
                <div className="action-menu">
                    <span role="button" className="btn-hide-widget" onClick={toggleIsMenuOpen} title="Hide widget">
                        <Icon icon="close" margin="m-0" className="lh-base" />
                    </span>
                    <div className="action-menu__header gap-2">
                        {activeTab !== "Help and resources" && (
                            <span
                                role="button"
                                className="action-menu__back"
                                onClick={() => setActiveTab("Help and resources")}
                                title="Back to Help and resources"
                            >
                                <Icon icon="arrow-thin-left" margin="m-0" />
                            </span>
                        )}
                        <h4 className="lh-base">{activeTab}</h4>
                    </div>
                    {activeTab === "Help and resources" && (
                        <>
                            <ul className="action-menu__list">
                                <li
                                    className="action-menu__list-item action-menu__list-item--highlight"
                                    role="button"
                                    title="Ask the community"
                                    onClick={() => setActiveTab("Ask the community")}
                                >
                                    <Icon icon="group" margin="m-0" />
                                    Ask the community
                                </li>
                                <li
                                    className="action-menu__list-item"
                                    role="button"
                                    title="Contact support"
                                    onClick={() => setActiveTab("Contact support")}
                                >
                                    <Icon icon="support" margin="m-0" />
                                    Contact support
                                </li>
                                <SeeDocumentationButton />
                            </ul>
                            <div className="action-menu__footer">
                                <div className="d-flex flex-row align-items-center">
                                    <small className="text-muted lh-1">See our project on GitHub</small>
                                    <FlexGrow />
                                    <GithubButtonWithStars />
                                </div>
                            </div>
                        </>
                    )}
                    {activeTab === "Ask the community" && (
                        <AskCommunityContent openFeedbackForm={() => setActiveTab("Submit feedback")} />
                    )}
                    {activeTab === "Submit feedback" && (
                        <FeedbackFormContent goBack={() => setActiveTab("Help and resources")} />
                    )}
                    {activeTab === "Contact support" && <ContactSupportContent />}
                </div>
            )}
        </section>
    );
}
