using System.Buffers;
using System.Text;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

internal sealed class WebhookActionExecutor(
    IHttpClientFactory httpClientFactory, ILogger<WebhookActionExecutor> logger)
{
    internal const string ClientName = "quill-action-webhook";

    internal const string SecretHeader = "X-Quill-Secret";

    internal const int MaxResponseBytes = 4 * 1024;

    internal const string TruncationMarker = "\n[truncated]";

    public async Task<string> ExecuteAsync(AiAgentActionRequest action, WebhookBinding binding, CancellationToken ct)
    {
        // one byte past the cap, so filling it is proof the body really was longer than the cap
        var buffer = ArrayPool<byte>.Shared.Rent(MaxResponseBytes + 1);
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, binding.Url);
            request.Content = new StringContent(action.Arguments ?? "{}", Encoding.UTF8, "application/json");

            if (string.IsNullOrEmpty(binding.Secret) == false)
                request.Headers.TryAddWithoutValidation(SecretHeader, binding.Secret);

            using var response = await httpClientFactory.CreateClient(ClientName).SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
            await using var stream = await response.Content.ReadAsStreamAsync(ct);

            var read = await stream.ReadAtLeastAsync(
                buffer.AsMemory(0, MaxResponseBytes + 1), MaxResponseBytes + 1, throwOnEndOfStream: false, ct);

            var truncated = read > MaxResponseBytes;
            if (truncated)
            {
                read = MaxResponseBytes;
            }

            if (Rune.DecodeLastFromUtf8(buffer.AsSpan(0, read), out _, out var partial) == OperationStatus.NeedMoreData)
            {
                read -= partial;
            }

            var body = Encoding.UTF8.GetString(buffer, 0, read);
            if (truncated)
            {
                body += TruncationMarker;
            }

            if (response.IsSuccessStatusCode)
            {
                if (body.Length == 0)
                    return "action succeeded: with no content";

                return body;
            }

            logger.LogWarning("Action webhook '{Action}' returned {Status}. Body: {Body}",
                action.Name, (int)response.StatusCode, body);
            return $"action failed: webhook returned {(int)response.StatusCode} body: {body}";
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
}
