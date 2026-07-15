namespace Raven.Quill.Contracts;

public sealed record ActivityEventDto(string Id, string AppId, string Type, string Message, DateTime Timestamp);
