namespace Raven.Quill.Contracts;

/// Bridge-to-web push for one inbound WhatsApp message. Internal contract: the
/// endpoint is excluded from OpenAPI and guarded by the shared bridge token.
public sealed record WhatsAppInboundRequest(
    string Database,
    string ChannelId,
    string Sender,
    string MessageId,
    string Kind,
    string? Text,
    long Timestamp);
