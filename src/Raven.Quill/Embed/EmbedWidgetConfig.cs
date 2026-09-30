using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;

namespace Raven.Quill.Embed;

/// One prior turn, slimmed to what the widget renders: the full conversation message carries timestamps,
/// usage and raw tool-call payloads that must never ship in a visitor-facing document.
internal sealed record HistoryTurn(AiMessageRole Role, string Content);

/// The payload the shell embeds as `<script type="application/json" id="rq-config">`. Mirrors
/// `WidgetConfig` in `packages/widget/src/widget-config.ts`.
internal sealed record EmbedWidgetConfig(
    string Mode,
    string ChatUrl,
    WidgetTheme Theme,
    HistoryTurn[] History);
