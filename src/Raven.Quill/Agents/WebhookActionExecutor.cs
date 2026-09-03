using System.Buffers;
using System.Text;
using Raven.Client.Documents.Operations.AI.Agents;
using Sparrow;

using Raven.Quill.Logging;

namespace Raven.Quill.Agents;

internal sealed class WebhookActionExecutor(
    IHttpClientFactory httpClientFactory, QuillLogger<WebhookActionExecutor> logger)
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

    private static readonly Size ResponseSizeLimit = new(256, SizeUnit.Kilobytes);
    private static readonly Size DefaultResponseSize = new(4, SizeUnit.Kilobytes);

    public async Task<string> ExecuteAsync(AiAgentActionRequest action, WebhookBinding binding, CancellationToken ct)
    {
        var size = binding.MaxResponseSize is > 0
            ? new Size(binding.MaxResponseSize.Value, SizeUnit.Bytes)
            : DefaultResponseSize;

        if (size > ResponseSizeLimit)
            size = ResponseSizeLimit;

        var maxResponseSize = (int)size.GetValue(SizeUnit.Bytes);

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
            if (logger.IsWarnEnabled)
                logger.Warn($"Action webhook '{action.Name}' timed out");
            return "action failed: webhook timed out";
        }
        catch (Exception e)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(e, $"Action webhook '{action.Name}' failure");
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
                description.Append(Environment.NewLine).Append(name).Append(": ").Append(string.Join(", ", values));
            }
        }

        if (body.Length > 0)
            description.Append(Environment.NewLine).Append(Environment.NewLine).Append(body);

        if (truncated)
            description.Append(Environment.NewLine).Append("[truncated]");

        return description.ToString();
    }
}
