import { useQuery } from "@tanstack/react-query";
import { Text } from "@/components/typography";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { FormFieldsSkeleton } from "@/components/data/loading-skeletons";
import { Alert } from "@/components/shadcn/ui/alert";
import { useChatWidgetThemeSave } from "@/pages/apps/channels/use-chat-widget-theme-save";
import { ChatWidgetThemeEditor } from "@/pages/apps/channels/chat-widget-theme-editor";

// The chat widget theme editor, rendered inside the channel detail's "Customize appearance" tab.
// Only mounted for embedded chat (IFrame) channels, whose theme endpoints exist.
export function ChatWidgetAppearanceTab({ slug, channelId }: { slug: string; channelId: string }) {
    const themeQuery = useQuery(api.queries.chatWidget.theme(slug, channelId));

    const saveMutation = useChatWidgetThemeSave({
        save: (theme) => api.services.iframe.updateTheme(slug, channelId, { theme }),
        invalidateKeys: [api.queries.chatWidget.theme(slug, channelId).queryKey],
        successMessage: "Theme saved",
    });

    return (
        <div className="grid gap-5">
            <Text variant="muted">
                Choose how this chat widget looks and reads. Pick an accent color and the rest of the palette is derived
                from it, so light and dark both stay coherent.
            </Text>

            <ApiState
                isLoading={themeQuery.isPending}
                isError={themeQuery.isError}
                errorTitle="Could not load the theme"
                onRetry={themeQuery.refetch}
                loadingLabel="Loading theme..."
                skeleton={<FormFieldsSkeleton count={4} />}
            >
                {themeQuery.data && (
                    <ChatWidgetThemeEditor
                        theme={themeQuery.data.theme}
                        defaultTheme={themeQuery.data.defaultTheme}
                        fontOptions={themeQuery.data.fontOptions}
                        canFollowAppDefault
                        isSaving={saveMutation.isPending}
                        onSave={(theme) => saveMutation.mutate(theme)}
                    />
                )}
            </ApiState>

            {saveMutation.isError && (
                <Alert variant="destructive">
                    {saveMutation.error instanceof Error ? saveMutation.error.message : "Could not save the theme."}
                </Alert>
            )}
        </div>
    );
}
