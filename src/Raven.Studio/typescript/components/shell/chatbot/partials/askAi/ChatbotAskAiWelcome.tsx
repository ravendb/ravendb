import { useAppSelector } from "components/store";
import { chatbotSelectors } from "../../store/chatbotSlice";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import AsciiLogo from "components/shell/chatbot/partials/askAi/iconAscii/IconAscii";
import { useEffect, useState } from "react";
import { Icon } from "components/common/Icon";
import IconName from "../../../../../../typings/server/icons";

const KEYWORDS: { word: string; icon?: IconName }[] = [
    { word: "Learn", icon: "learn" },
    { word: "Query", icon: "query" },
    { word: "Analyze", icon: "analyze" },
    { word: "Troubleshoot", icon: "hammer-driver" },
];

export default function ChatbotAskAiWelcome() {
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const messagesCount = useAppSelector(chatbotSelectors.messagesCount);

    const [index, setIndex] = useState(0);
    const [offset, setOffset] = useState(20);
    const [opacity, setOpacity] = useState(0);
    const [hasStarted, setHasStarted] = useState(false);

    useEffect(() => {
        const startTimeout = setTimeout(() => {
            setOffset(0);
            setOpacity(1);
            setHasStarted(true);
        }, 1000);

        return () => clearTimeout(startTimeout);
    }, []);

    useEffect(() => {
        if (!hasStarted) return;

        const interval = setInterval(() => {
            setOffset(-20);
            setOpacity(0);

            setTimeout(() => {
                setIndex((prev) => (prev + 1) % KEYWORDS.length);
                setOffset(20);
                setTimeout(() => {
                    setOffset(0);
                    setOpacity(1);
                }, 50);
            }, 400);
        }, 3000);

        return () => clearInterval(interval);
    }, [hasStarted]);

    const isConsentSuccess = consentStatus.data === "Success";

    if (messagesCount > 0 || !isConsentSuccess) {
        return null;
    }

    const { word, icon } = KEYWORDS[index];

    return (
        <div className="p-5 text-center">
            <AsciiLogo />
            <div className="vstack gap-1">
                <h3 className="mt-4 mb-0 fw-semibold">AI Assistant</h3>
                <div className="d-flex justify-content-center align-items-center overflow-hidden">
                    <h3
                        style={{
                            transition: offset === 20 ? "none" : "all 0.4s cubic-bezier(0.4, 0, 0.2, 1)",
                            transform: `translateY(${offset}px)`,
                            opacity: opacity,
                            display: "inline-block",
                        }}
                        className="font-monospace well rounded-pill fw-bold py-1 px-2 mb-0 font-size-16"
                    >
                        {icon && <Icon icon={icon} className="text-primary" />}
                        {word}
                    </h3>
                </div>
            </div>
        </div>
    );
}
