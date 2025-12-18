import { useEffect, useState } from "react";

interface UseTypewriterOptions {
    text: string;
    delay?: number;
    isDone?: boolean;
}

export default function useTypewriter({ text, delay = 0, isDone = false }: UseTypewriterOptions): string {
    const [displayedText, setDisplayedText] = useState("");

    useEffect(() => {
        if (isDone) {
            setDisplayedText(text);
            return;
        }

        const timeout = setTimeout(() => {
            const nextText = (text ?? "").slice(0, displayedText.length + 1);
            setDisplayedText(nextText);
        }, delay);

        return () => {
            clearTimeout(timeout);
        };
    }, [text, displayedText.length, isDone]);

    return displayedText;
}
