namespace Raven.Quill.Endpoints.Helpers;

internal static class ChatLimits
{
    internal const int MaxPromptLength = 32_000;
    internal const long MaxBodyBytes = 256 * 1024;
}
