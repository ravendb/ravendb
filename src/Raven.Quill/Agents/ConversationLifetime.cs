namespace Raven.Quill.Agents;

public sealed record ConversationLifetime(TimeSpan? TranscriptIdleWindow, TimeSpan? PreviewRetention);
