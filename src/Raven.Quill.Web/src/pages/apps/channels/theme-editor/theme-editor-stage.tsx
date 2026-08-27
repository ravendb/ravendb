import { Hand, MessagesSquare, Monitor, Moon, Smartphone, Sun, TriangleAlert, UnfoldHorizontal } from "lucide-react";
import { useState } from "react";
import type { WidgetTheme } from "@/api/generated/server-api";
import { Separator } from "@/components/shadcn/ui/separator";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import { Text } from "@/components/typography";
import {
    WebWidgetThemePreview,
    type PreviewAppearance,
    type PreviewView,
} from "@/pages/apps/channels/web-widget-theme-preview";

// The widths an embedded widget actually gets: a phone-width column, the panel a desktop bubble opens
// at, and whatever the host page gives an inline embed. The exact pixels stay in the tooltips, where
// they answer "how narrow is narrow?" without turning the control into a row of numbers.
const PREVIEW_WIDTHS = [
    { value: "320", label: "Mobile", title: "Mobile - 320px", icon: Smartphone },
    { value: "480", label: "Desktop", title: "Desktop - 480px", icon: Monitor },
    { value: "fill", label: "Fill", title: "Fill the available width", icon: UnfoldHorizontal },
] as const;

type PreviewWidth = (typeof PREVIEW_WIDTHS)[number]["value"];

type ThemeEditorStageProps = {
    previewTheme: WidgetTheme;
    previewAppearance: PreviewAppearance;
    onPreviewAppearanceChange: (next: PreviewAppearance) => void;
    previewView: PreviewView;
    onPreviewViewChange: (next: PreviewView) => void;
};

export function ThemeEditorStage({
    previewTheme,
    previewAppearance,
    onPreviewAppearanceChange,
    previewView,
    onPreviewViewChange,
}: ThemeEditorStageProps) {
    // Today's fixed width, kept as the default so the preview looks the same as before until an
    // operator asks for something narrower or wider.
    const [previewWidth, setPreviewWidth] = useState<PreviewWidth>("480");

    return (
        // overflow-hidden: a stage that can't actually fit its content (a fixed-height frame, a chosen
        // width wider than the space available) must clip locally rather than push the shell's outlet
        // wrapper into scrolling the whole page.
        // @container/stage: the "clamped to fit" note below needs the stage's own rendered width, which
        // is exactly what max-w-full is clamping the frame against a few lines down.
        // @5xl/theme-editor bounds this to fill its share of the two-pane split only once that split is
        // actually active; below it the stage keeps its natural height like the inspector does.
        <div className="@container/stage relative flex shrink-0 flex-col overflow-hidden dot-grid @5xl/theme-editor:min-h-0 @5xl/theme-editor:flex-1">
            {/* The controls float on the canvas rather than sitting in a bar above it: they steer the
                preview, so they read as tools over the artwork instead of as another row of form fields.
                Top-centred, not bottom-centred, because the frame below fills the canvas height and a
                bottom pill would cover the widget's own composer. */}
            <div className="absolute inset-x-0 top-3 z-10 flex justify-center px-3">
                <div className="flex max-w-full flex-wrap items-center justify-center gap-1 rounded-xl border bg-card/85 p-1 shadow-lg backdrop-blur-sm">
                    <ToggleGroup
                        type="single"
                        size="sm"
                        spacing={0}
                        value={previewView}
                        onValueChange={(next) => {
                            if (next !== "") onPreviewViewChange(next as PreviewView);
                        }}
                        aria-label="Previewed screen"
                    >
                        <ToggleGroupItem value="Welcome" aria-label="Welcome" title="Welcome screen">
                            <Hand aria-hidden="true" />
                        </ToggleGroupItem>
                        <ToggleGroupItem value="Conversation" aria-label="Conversation" title="Conversation">
                            <MessagesSquare aria-hidden="true" />
                        </ToggleGroupItem>
                    </ToggleGroup>
                    <Separator orientation="vertical" className="mx-1 h-5 data-[orientation=vertical]:self-center" />
                    <ToggleGroup
                        type="single"
                        size="sm"
                        spacing={0}
                        value={previewAppearance}
                        onValueChange={(next) => {
                            if (next !== "") onPreviewAppearanceChange(next as PreviewAppearance);
                        }}
                        aria-label="Previewed color scheme"
                    >
                        <ToggleGroupItem value="Light" aria-label="Light" title="Light">
                            <Sun aria-hidden="true" />
                        </ToggleGroupItem>
                        <ToggleGroupItem value="Dark" aria-label="Dark" title="Dark">
                            <Moon aria-hidden="true" />
                        </ToggleGroupItem>
                    </ToggleGroup>
                    <Separator orientation="vertical" className="mx-1 h-5 data-[orientation=vertical]:self-center" />
                    <ToggleGroup
                        type="single"
                        size="sm"
                        spacing={0}
                        value={previewWidth}
                        onValueChange={(next) => {
                            if (next !== "") setPreviewWidth(next as PreviewWidth);
                        }}
                        aria-label="Preview width"
                    >
                        {PREVIEW_WIDTHS.map(({ value, label, title, icon: Icon }) => (
                            <ToggleGroupItem key={value} value={value} aria-label={label} title={title}>
                                <Icon aria-hidden="true" />
                            </ToggleGroupItem>
                        ))}
                    </ToggleGroup>
                    {/* The chip's whole point is fidelity at an exact width - silently clamping it with
                        max-w-full below would make it lie. Each breakpoint is a literal Tailwind class
                        (the scanner can't see a value built from `${width}px`), so one span per numeric
                        width rather than one driven off PREVIEW_WIDTHS. */}
                    {previewWidth === "320" && (
                        // @max-[Npx] compiles to "width < Npx", not "width <= Npx" - so 320, not 319, is
                        // the boundary that catches every width the 320px preview actually gets clamped
                        // at (0-319px) while staying hidden at exactly 320px, where it's honored exactly.
                        <Text
                            as="span"
                            variant="caption"
                            className="hidden items-center gap-1 px-1.5 @max-[320px]/stage:inline-flex"
                        >
                            <TriangleAlert className="size-3" aria-hidden="true" />
                            Clamped to fit
                        </Text>
                    )}
                    {previewWidth === "480" && (
                        <Text
                            as="span"
                            variant="caption"
                            className="hidden items-center gap-1 px-1.5 @max-[480px]/stage:inline-flex"
                        >
                            <TriangleAlert className="size-3" aria-hidden="true" />
                            Clamped to fit
                        </Text>
                    )}
                </div>
            </div>
            {/* pt-16 keeps the frame clear of the floating pill above it at every width. */}
            <div className="flex min-w-0 items-start justify-center p-4 pt-16 @5xl/theme-editor:min-h-0 @5xl/theme-editor:flex-1 @5xl/theme-editor:items-stretch">
                {/* This row's main axis is horizontal (width), so it stretches the box's height for free
                    via the parent's items-stretch above - flex-1/min-h-0 here would fight the fixed
                    width instead of filling height, so they stay off this element. */}
                <div
                    style={{ width: previewWidth === "fill" ? "100%" : `${previewWidth}px` }}
                    className="flex max-w-full flex-col"
                >
                    <WebWidgetThemePreview
                        theme={previewTheme}
                        appearance={previewAppearance}
                        view={previewView}
                        // Fixed height matches the stacked layout's natural sizing (F2); once the two-pane
                        // split is active the frame instead fills whatever height the stage actually has,
                        // which is what was overflowing it before.
                        className="h-[640px] @5xl/theme-editor:h-auto @5xl/theme-editor:min-h-0 @5xl/theme-editor:flex-1"
                    />
                </div>
            </div>
        </div>
    );
}
