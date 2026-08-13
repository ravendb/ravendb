import { ChatWidget } from "@/chat-widget";
import { useResolvedAppearance } from "@/use-resolved-appearance";
import type { WidgetConfig } from "@/widget-config";

export function LiveApp({ config }: { config: WidgetConfig }) {
    const appearance = useResolvedAppearance(config.theme.appearance);

    return (
        <ChatWidget theme={config.theme} appearance={appearance} history={config.history} chatUrl={config.chatUrl} />
    );
}
