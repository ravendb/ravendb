import { Composer } from "@/components/composer";
import { Greeting } from "@/components/greeting";
import { MessageFeed } from "@/components/message-feed";
import { MessageList } from "@/components/message-list";
import { WidgetHeader } from "@/components/widget-header";
import { useChat, type ChatMessage } from "@/use-chat";
import type { HistoryTurn } from "@/widget-config";
import { widgetThemeStyle, type ResolvedAppearance, type WidgetTheme } from "@/widget-theme";

type ChatWidgetViewProps = {
    theme: WidgetTheme;
    appearance: ResolvedAppearance;
    messages: ChatMessage[];
    streamingId: string | null;
    errorMessage: string | null;
    /** Locks the composer for a state the visitor can't recover from (expired link, exhausted budget). */
    isBlocked: boolean;
    timeLabel?: string | null;
    onSubmit: (prompt: string) => void;
    onStop: () => void;
};

/** The whole widget as a pure function of its props, so every state - empty, streaming, error, expired - is a
 *  prop combination a story or a test can render directly. */
export function ChatWidgetView({
    theme,
    appearance,
    messages,
    streamingId,
    errorMessage,
    isBlocked,
    timeLabel = null,
    onSubmit,
    onStop,
}: ChatWidgetViewProps) {
    const hasTranscript = messages.length > 0;

    // Grows with every appended chunk, which is what tells the feed there is new content to follow.
    const scrollSignal = messages.reduce((total, message) => total + message.content.length, messages.length);

    return (
        <div
            style={widgetThemeStyle(theme, appearance)}
            className="font-rq bg-rq-bg text-rq-fg flex h-full flex-col antialiased"
        >
            {theme.showHeader && (
                <WidgetHeader title={theme.headerTitle} subtitle={theme.headerSubtitle} logo={theme.logo} />
            )}

            <MessageFeed scrollSignal={hasTranscript ? scrollSignal : "empty"}>
                {hasTranscript ? (
                    <MessageList
                        messages={messages}
                        streamingId={streamingId}
                        timeLabel={timeLabel}
                        errorMessage={errorMessage}
                    />
                ) : (
                    <Greeting
                        title={theme.greetingTitle}
                        body={theme.greetingBody}
                        suggestedPrompts={theme.suggestedPrompts}
                        isDisabled={isBlocked}
                        onSelectPrompt={onSubmit}
                    />
                )}
            </MessageFeed>

            <Composer
                placeholder={theme.inputPlaceholder}
                isStreaming={streamingId !== null}
                isDisabled={isBlocked}
                onSubmit={onSubmit}
                onStop={onStop}
            />

            {theme.disclaimer !== null && theme.disclaimer.length > 0 && (
                <p className="text-rq-muted shrink-0 px-[var(--rq-pad-x)] pb-2.5 text-center text-[0.6875rem]">
                    {theme.disclaimer}
                </p>
            )}
        </div>
    );
}

type ChatWidgetProps = {
    theme: WidgetTheme;
    appearance: ResolvedAppearance;
    history: HistoryTurn[];
    /** Null renders an inert widget: the transcript and every control show, nothing calls the server. */
    chatUrl: string | null;
    timeLabel?: string | null;
};

export function ChatWidget({ theme, appearance, history, chatUrl, timeLabel = null }: ChatWidgetProps) {
    const chat = useChat(chatUrl, history);

    return (
        <ChatWidgetView
            theme={theme}
            appearance={appearance}
            messages={chat.messages}
            streamingId={chat.streamingId}
            errorMessage={chat.errorMessage}
            isBlocked={chat.isBlocked}
            timeLabel={timeLabel}
            onSubmit={chat.send}
            onStop={chat.stop}
        />
    );
}
