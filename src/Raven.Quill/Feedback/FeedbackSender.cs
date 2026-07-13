using System.Reflection;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Client.Documents;

namespace Raven.Quill.Feedback;

internal sealed class FeedbackSender(IAiHelperClient ravendb) : IFeedbackSender
{
    private const string FeedbackPath = "/studio/feedback";
    private const string ProductName = "RavenDB";
    private const string FeatureName = "Quill";

    private static readonly string ProductVersion = GetVersion(typeof(IDocumentStore).Assembly);
    private static readonly string StudioVersion = GetVersion(typeof(FeedbackSender).Assembly);

    public async Task<bool> SendAsync(SendFeedbackRequest request, string userAgent, CancellationToken token)
    {
        RavenFeedbackForm feedback = new(
            request.Message,
            new RavenFeedbackProduct(
                ProductName,
                ProductVersion,
                StudioVersion,
                request.StudioView,
                FeatureName,
                request.Impression),
            new RavenFeedbackUser(request.Name, request.Email, userAgent));

        (AiHelperStatus transport, _) = await ravendb.SendAsync(FeedbackPath, "POST", feedback, token);
        return transport == AiHelperStatus.Success;
    }

    private static string GetVersion(Assembly assembly) =>
        assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ??
        assembly.GetName().Version?.ToString() ??
        "unknown";

    private sealed record RavenFeedbackForm(
        string Message,
        RavenFeedbackProduct Product,
        RavenFeedbackUser User);

    private sealed record RavenFeedbackProduct(
        string Name,
        string Version,
        string StudioVersion,
        string? StudioView,
        string FeatureName,
        string? FeatureImpression);

    private sealed record RavenFeedbackUser(
        string Name,
        string Email,
        string UserAgent);
}
