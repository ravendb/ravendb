import { ChatbotRelevantLink } from "commands/aiAssistant/runChatbotAiAssistantCommand";
import "./ChatbotAskAiMessageRelevantLinks.scss";

const sourceChipFavicon = require("Content/img/source-chip-favicon.svg");

interface ChatbotAskAiMessageRelevantLinksProps {
    links: ChatbotRelevantLink[];
}

export default function ChatbotAskAiMessageRelevantLinks({ links }: ChatbotAskAiMessageRelevantLinksProps) {
    if (!links?.length) {
        return null;
    }

    return (
        <div className="pb-2 vstack gap-1">
            <span className="small-label">Sources</span>
            <div className="hstack gap-1 flex-wrap pb-2">
                {links.filter(Boolean).map((link) => (
                    <a
                        key={link.Url}
                        href={link.Url}
                        target="_blank"
                        className="source-chip no-decor"
                        title={link.Title}
                    >
                        <img src={sourceChipFavicon} width={12} />
                        <span>{link.Title}</span>
                    </a>
                ))}
            </div>
        </div>
    );
}
