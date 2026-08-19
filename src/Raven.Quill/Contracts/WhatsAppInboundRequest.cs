namespace Raven.Quill.Contracts;

public sealed record WhatsAppInboundRequest(
    string Database,
    string ChannelId,
    string Sender,
    string MessageId,
    string Kind,
    string? Text,
    long Timestamp);
