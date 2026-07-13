namespace Raven.Quill.Contracts;

public sealed record SendFeedbackRequest(string Name, string Email, string? Impression, string Message, string? StudioView);
