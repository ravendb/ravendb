import { ChatbotRelevantLink } from "commands/aiAssistant/runChatbotAiAssistantCommand";
import { Icon } from "components/common/Icon";

interface ChatbotAskAiMessageRelevantLinksProps {
    links: ChatbotRelevantLink[];
}

export default function ChatbotAskAiMessageRelevantLinks({ links }: ChatbotAskAiMessageRelevantLinksProps) {
    if (!links?.length) {
        return null;
    }

    return (
        <div className="hstack gap-1 flex-wrap mt-1">
            {links.filter(Boolean).map((link) => (
                <a
                    key={link.Url}
                    href={link.Url}
                    target="_blank"
                    className="btn btn-sm rounded-pill py-1 px-2 panel-bg-2 border border-secondary text-reset"
                >
                    <Icon icon="raven" size="sm" color="info" />
                    {link.Title}
                </a>
            ))}
        </div>
    );
}
