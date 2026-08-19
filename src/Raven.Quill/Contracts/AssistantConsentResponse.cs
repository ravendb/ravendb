using Raven.Quill.AiHelper;

namespace Raven.Quill.Contracts;

/// <summary>How the AI service answers a consent check or grant. <c>ConsentRequired</c> and
/// <c>InvalidCredentials</c> are answers rather than failures, so they arrive as a 200 with the
/// status instead of the service's own 401 — which the browser reads as a lost Quill session.</summary>
public sealed record AssistantConsentResponse(AiHelperStatus Status);
