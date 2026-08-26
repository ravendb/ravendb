import { useState } from "react";
import type { WidgetTheme } from "@/api/generated/server-api";
import { Separator } from "@/components/shadcn/ui/separator";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import {
    WebWidgetThemePreview,
    type PreviewAppearance,
    type PreviewView,
} from "@/pages/apps/channels/web-widget-theme-preview";

const PREVIEW_WIDTHS = [
    { value: "320", label: "320" },
    { value: "480", label: "480" },
    { value: "fill", label: "Fill" },
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
        <div className="flex min-h-0 flex-1 flex-col gap-3 p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
                <span className="text-sm font-medium">Live preview</span>
                <div className="flex flex-wrap items-center gap-3">
                    <ToggleGroup
                        type="single"
                        variant="outline"
                        size="sm"
                        value={previewView}
                        onValueChange={(next) => {
                            if (next !== "") onPreviewViewChange(next as PreviewView);
                        }}
                        aria-label="Previewed screen"
                    >
                        <ToggleGroupItem value="Welcome">Welcome</ToggleGroupItem>
                        <ToggleGroupItem value="Conversation">Conversation</ToggleGroupItem>
                    </ToggleGroup>
                    <Separator orientation="vertical" />
                    <ToggleGroup
                        type="single"
                        variant="outline"
                        size="sm"
                        value={previewAppearance}
                        onValueChange={(next) => {
                            if (next !== "") onPreviewAppearanceChange(next as PreviewAppearance);
                        }}
                        aria-label="Previewed color scheme"
                    >
                        <ToggleGroupItem value="Light">Light</ToggleGroupItem>
                        <ToggleGroupItem value="Dark">Dark</ToggleGroupItem>
                    </ToggleGroup>
                    <Separator orientation="vertical" />
                    <ToggleGroup
                        type="single"
                        variant="outline"
                        size="sm"
                        value={previewWidth}
                        onValueChange={(next) => {
                            if (next !== "") setPreviewWidth(next as PreviewWidth);
                        }}
                        aria-label="Preview width"
                    >
                        {PREVIEW_WIDTHS.map((width) => (
                            <ToggleGroupItem key={width.value} value={width.value}>
                                {width.label}
                            </ToggleGroupItem>
                        ))}
                    </ToggleGroup>
                </div>
            </div>
            <div className="flex min-h-0 flex-1 items-start justify-center">
                <div style={{ width: previewWidth === "fill" ? "100%" : `${previewWidth}px` }} className="max-w-full">
                    <WebWidgetThemePreview theme={previewTheme} appearance={previewAppearance} view={previewView} />
                </div>
            </div>
        </div>
    );
}
