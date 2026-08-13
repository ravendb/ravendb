using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;

namespace Raven.Quill.Embed;

/// The payload the shell embeds as `<script type="application/json" id="rq-config">`. Mirrors
/// `WidgetConfig` in `packages/widget/src/widget-config.ts`.
internal sealed record EmbedWidgetConfig(
    string Mode,
    string ChatUrl,
    WidgetTheme Theme,
    AiConversationMessage[] History);
