import "./ChatbotAskAiKeywordsSlider.scss";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { chatbotSelectors } from "components/shell/chatbot/store/chatbotSlice";
import { useAppSelector } from "components/store";
import { useState, useEffect } from "react";
import IconName from "typings/server/icons";

const KEYWORDS: { word: string; icon?: IconName }[] = [
    { word: "Learn", icon: "learn" },
    { word: "Query", icon: "query" },
    { word: "Analyze", icon: "analyze" },
    { word: "Troubleshoot", icon: "hammer-driver" },
] as const;

export default function KeywordsSlider() {
    const [index, setIndex] = useState(0);
    const [isVisible, setIsVisible] = useState(true);
    const isChatbotOpen = useAppSelector(chatbotSelectors.isOpen);

    useEffect(() => {
        if (!isChatbotOpen) {
            return;
        }

        const id = setInterval(() => {
            setIsVisible(false);

            setTimeout(() => {
                setIndex((prev) => (prev + 1) % KEYWORDS.length);
                setIsVisible(true);
            }, 300);
        }, 3000);

        return () => clearInterval(id);
    }, [isChatbotOpen]);

    const { word, icon } = KEYWORDS[index];

    return (
        <div className="d-flex justify-content-center align-items-center overflow-hidden">
            <h3
                className={classNames(
                    "keyword-pill font-monospace well rounded-pill fw-bold py-1 px-2 mb-0 font-size-16",
                    isVisible ? "is-visible" : "is-hidden"
                )}
            >
                {icon && <Icon icon={icon} className="text-primary" />}
                {word}
            </h3>
        </div>
    );
}
