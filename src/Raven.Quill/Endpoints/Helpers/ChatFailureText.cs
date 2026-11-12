using Raven.Quill.Agents;

namespace Raven.Quill.Endpoints.Helpers;

internal static class ChatFailureText
{
    internal static string ForOperator(ProviderFailure failure, Exception exception)
    {
        var reason = RavenErrorText.Reason(exception);
        return string.IsNullOrWhiteSpace(reason) ? failure.OperatorMessage : $"{failure.OperatorMessage}: {reason}";
    }
}
