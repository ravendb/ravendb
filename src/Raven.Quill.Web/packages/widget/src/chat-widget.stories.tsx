import type { Meta, StoryObj } from "@storybook/react-vite";
import { ChatWidgetView } from "@/chat-widget";
import { PREVIEW_TRANSCRIPT } from "@/preview-transcript";
import type { ChatMessage } from "@/use-chat";
import { DEFAULT_THEME } from "@/widget-theme";

const THEME = {
    ...DEFAULT_THEME,
    light: { buttonColor: "#2f6f4f", messageColor: "#e6eeea", backgroundColor: "#ffffff" },
    dark: { buttonColor: "#2f6f4f", messageColor: "#152824", backgroundColor: "#0d1117" },
    headerTitle: "Order support",
    headerSubtitle: "We usually reply instantly",
    greetingTitle: "Need a hand with an order?",
    greetingBody: "Ask about delivery, returns or anything else.",
    suggestedPrompts: ["Where is my order?", "How do I return an item?", "Do you ship internationally?"],
    disclaimer: "Answers are AI generated and may be inaccurate.",
};

const MINIMAL_THEME = {
    ...THEME,
    showHeader: false,
    headerSubtitle: null,
    greetingTitle: null,
    greetingBody: null,
    suggestedPrompts: [],
    disclaimer: null,
};

/** Prose, a GFM table and a fenced code block - the content an operator judges a theme by. */
const TRANSCRIPT: ChatMessage[] = PREVIEW_TRANSCRIPT.map((turn, index) => ({ id: `s${index}`, ...turn }));

const PENDING_TURN: ChatMessage[] = [
    { id: "u1", role: "user", content: "Where is my order?" },
    { id: "a1", role: "assistant", content: "" },
];

const meta = {
    title: "Widget/Chat",
    component: ChatWidgetView,
    // The widget always fills its iframe, so a fixed frame is what makes the stories comparable.
    decorators: [
        (Story) => (
            <div className="mx-auto mt-6 h-[640px] w-[420px] overflow-hidden rounded-xl border border-neutral-300">
                <Story />
            </div>
        ),
    ],
    args: {
        theme: THEME,
        appearance: "Light",
        messages: [],
        streamingId: null,
        errorMessage: null,
        isBlocked: false,
        timeLabel: "Today",
        onSubmit: () => {},
        onStop: () => {},
    },
} satisfies Meta<typeof ChatWidgetView>;

export default meta;

type Story = StoryObj<typeof meta>;

// Every state ships in both appearances: the palette is derived rather than authored, so a regression tends
// to show in exactly one of the two.

export const EmptyLight: Story = {};
export const EmptyDark: Story = { args: { appearance: "Dark" } };

export const MarkdownLight: Story = { args: { messages: TRANSCRIPT } };
export const MarkdownDark: Story = { args: { messages: TRANSCRIPT, appearance: "Dark" } };

// A half-written fence and an unterminated bold: `remend` closes both before react-markdown sees them.
const STREAMING_MESSAGES: ChatMessage[] = [
    { id: "u1", role: "user", content: "Summarise the plan comparison." },
    { id: "a1", role: "assistant", content: "Sure. The **Team plan is the one that\n\n```ts\nconst seats = 25" },
];

export const StreamingLight: Story = { args: { messages: STREAMING_MESSAGES, streamingId: "a1" } };
export const StreamingDark: Story = {
    args: { messages: STREAMING_MESSAGES, streamingId: "a1", appearance: "Dark" },
};

export const ThinkingLight: Story = { args: { messages: PENDING_TURN, streamingId: "a1" } };
export const ThinkingDark: Story = { args: { messages: PENDING_TURN, streamingId: "a1", appearance: "Dark" } };

export const ErrorLight: Story = {
    args: { messages: PENDING_TURN, errorMessage: "Something went wrong. Please try again." },
};
export const ErrorDark: Story = {
    args: { messages: PENDING_TURN, errorMessage: "Something went wrong. Please try again.", appearance: "Dark" },
};

export const LinkExpiredLight: Story = {
    args: { messages: PENDING_TURN, errorMessage: "This conversation link is no longer active.", isBlocked: true },
};
export const LinkExpiredDark: Story = {
    args: {
        messages: PENDING_TURN,
        errorMessage: "This conversation link is no longer active.",
        isBlocked: true,
        appearance: "Dark",
    },
};

export const LimitReachedLight: Story = {
    args: { messages: TRANSCRIPT, errorMessage: "This conversation has reached its usage limit.", isBlocked: true },
};
export const LimitReachedDark: Story = {
    args: {
        messages: TRANSCRIPT,
        errorMessage: "This conversation has reached its usage limit.",
        isBlocked: true,
        appearance: "Dark",
    },
};

// Nothing optional set: no header, greeting, prompts or disclaimer - the leanest widget an operator can
// configure, and the one most likely to expose a missing fallback.
export const MinimalLight: Story = { args: { theme: MINIMAL_THEME, timeLabel: null } };
export const MinimalDark: Story = { args: { theme: MINIMAL_THEME, timeLabel: null, appearance: "Dark" } };
