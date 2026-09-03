import { useEffect, useState } from "react";
import { ChatWidgetView } from "@/chat-widget";
import { readHostAppearance } from "@/host-channel";
import { useChat } from "@/use-chat";
import { useHostChannel } from "@/use-host-channel";
import { useResolvedAppearance } from "@/use-resolved-appearance";
import type { WidgetConfig } from "@/widget-config";
import type { WidgetAppearance } from "@/widget-theme";

/** Live mode owns the chat controller directly rather than going through `ChatWidget`, because it is the
 *  one mode that also reports the controller's terminal states out to the host page. */
export function LiveApp({ config }: { config: WidgetConfig }) {
    // A host page with its own theme toggle overrides the saved appearance by posting an `appearance`
    // message; until (and unless) one arrives, the operator's setting stands.
    const [hostAppearance, setHostAppearance] = useState<WidgetAppearance | null>(null);
    const appearance = useResolvedAppearance(hostAppearance ?? config.theme.appearance);
    const chat = useChat(config.chatUrl, config.history);

    useHostChannel(chat.errorKind, chat.errorMessage);

    useEffect(() => {
        const onMessage = (event: MessageEvent) => {
            const next = readHostAppearance(event);
            if (next !== null) setHostAppearance(next);
        };

        window.addEventListener("message", onMessage);
        return () => window.removeEventListener("message", onMessage);
    }, []);

    return (
        <ChatWidgetView
            theme={config.theme}
            appearance={appearance}
            messages={chat.messages}
            streamingId={chat.streamingId}
            errorMessage={chat.errorMessage}
            isBlocked={chat.isBlocked}
            onSubmit={chat.send}
            onStop={chat.stop}
        />
    );
}
