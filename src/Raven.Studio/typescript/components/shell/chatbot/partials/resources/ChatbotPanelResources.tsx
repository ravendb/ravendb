import "./ChatbotPanelResources.scss";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import SeeDocumentationButton from "./partials/SeeDocumentationButton";
import GithubButtonWithStars from "./partials/GithubButtonWithStars";
import { JoinCommunityContent } from "./partials/JoinCommunityContent";
import { FeedbackFormContent } from "./partials/FeedbackFormContent";
import { ContactSupportContent } from "./partials/ContactSupportContent";
import { chatbotActions, chatbotSelectors } from "../../store/chatbotSlice";
import { useAppDispatch, useAppSelector } from "components/store";

export default function ChatbotPanelResources() {
    const dispatch = useAppDispatch();
    const activeTab = useAppSelector(chatbotSelectors.chatbotResourcesTab);

    return (
        <div className="chatbot-panel-resources vstack">
            {activeTab === "helpAndResources" && (
                <div className="vstack">
                    <ul className="action-menu__list flex-grow">
                        <p>
                            Explore our documentation, engage with community, and connect with our dedicated support
                            channels for all your needs.
                        </p>
                        <li
                            className="action-menu__list-item action-menu__list-item--highlight"
                            role="button"
                            title="Join the Community"
                            onClick={() => dispatch(chatbotActions.chatbotResourcesTabSet("joinTheCommunity"))}
                        >
                            <Icon icon="group" margin="m-0" />
                            Join the Community
                        </li>
                        <li
                            className="action-menu__list-item"
                            role="button"
                            title="Contact support"
                            onClick={() => dispatch(chatbotActions.chatbotResourcesTabSet("contactSupport"))}
                        >
                            <Icon icon="support" margin="m-0" />
                            Contact support
                        </li>
                        <SeeDocumentationButton />
                    </ul>
                    <div className="action-menu__footer">
                        <div className="d-flex flex-row align-items-center">
                            <small className="lh-1">See our project on GitHub</small>
                            <FlexGrow />
                            <GithubButtonWithStars />
                        </div>
                    </div>
                </div>
            )}
            {activeTab === "joinTheCommunity" && (
                <JoinCommunityContent
                    openFeedbackForm={() => dispatch(chatbotActions.chatbotResourcesTabSet("submitFeedback"))}
                />
            )}
            {activeTab === "submitFeedback" && (
                <FeedbackFormContent
                    goBack={() => dispatch(chatbotActions.chatbotResourcesTabSet("helpAndResources"))}
                />
            )}
            {activeTab === "contactSupport" && <ContactSupportContent />}
        </div>
    );
}
