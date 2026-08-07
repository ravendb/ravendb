using System.Buffers;
using System.Text;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

internal sealed class WebhookActionExecutor(
    IHttpClientFactory httpClientFactory, ILogger<WebhookActionExecutor> logger)
{
    internal const string ClientName = "quill-action-webhook";

    internal const string SecretHeader = "X-Quill-Secret";

    private static readonly string[] NotableHeaders =
    [
        "RateLimit-Limit",
        "X-RateLimit-Limit",
        "RateLimit-Remaining",
        "X-RateLimit-Remaining",
        "RateLimit-Reset",
        "Retry-After",
        "Deprecation",
        "Sunset",
        "Link",
        "X-Request-ID",
        "X-Correlation-ID",
        "Date",
        "Last-Modified",
        "Location"
    ];

    public async Task<string> ExecuteAsync(AiAgentActionRequest action, WebhookBinding binding, CancellationToken ct)
    {
        var maxResponseSize = binding.MaxResponseSize ?? 4 * 1024;
        if (maxResponseSize <= 0)
            maxResponseSize = 4 * 1024;
        if (maxResponseSize > 256 * 1024)
            maxResponseSize = 256 * 1024;

        var buffer = ArrayPool<byte>.Shared.Rent(maxResponseSize);
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, binding.Url);
            request.Content = new StringContent(action.Arguments ?? "{}", Encoding.UTF8, "application/json");

            if (string.IsNullOrEmpty(binding.Secret) == false)
                request.Headers.TryAddWithoutValidation(SecretHeader, binding.Secret);

            using var response = await httpClientFactory.CreateClient(ClientName).SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
            await using var stream = await response.Content.ReadAsStreamAsync(ct);

            var read = await stream.ReadAtLeastAsync(
                buffer.AsMemory(0, maxResponseSize), maxResponseSize, throwOnEndOfStream: false, ct);

            var truncated = read == maxResponseSize;
            if (Rune.DecodeLastFromUtf8(buffer.AsSpan(0, read), out _, out var partial) == OperationStatus.NeedMoreData)
            {
                read -= partial;
            }

            var body = Encoding.UTF8.GetString(buffer, 0, read);
            return Describe(response, body, truncated);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            logger.LogWarning("Action webhook '{Action}' timed out", action.Name);
            return "action failed: webhook timed out";
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "Action webhook '{Action}' failure", action.Name);
            return $"action failed: {e.Message}";
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
   
    private static string Describe(HttpResponseMessage response, string body, bool truncated)
    {
        var outcome = response.IsSuccessStatusCode ? "succeeded" : "failed";
        var description =
            new StringBuilder($"action {outcome}: webhook returned {(int)response.StatusCode}");

        if (body.Length == 0)
            description.Append(" with no content");

        foreach (var name in NotableHeaders)
        {
            if (response.Headers.TryGetValues(name, out var values) ||
                response.Content.Headers.TryGetValues(name, out values))
            {
                description.Append('\n').Append(name).Append(": ").Append(string.Join(", ", values));
            }
        }

        if (body.Length > 0)
            description.Append("\n\n").Append(body);

        if (truncated)
            description.Append("\n[truncated]");

        return description.ToString();
    }
}
