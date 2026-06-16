using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.AiAppliance.Agents;

/// <summary>
/// Runs a single, non-persisted conversation turn against a <em>draft</em>
/// <see cref="AiAgentConfiguration"/> via RavenDB's <c>POST /databases/{db}/ai/agent/test</c>
/// endpoint — the same endpoint Studio's agent test panel uses — and streams the reply.
/// This lets the wizard smoke-test the configuration an operator is still editing in the
/// Review step, before it is provisioned (so there is no persisted agent to bind to via
/// <see cref="AgentRouter"/>). The endpoint streams the reply property (resolved via
/// <see cref="AgentOutputShape.ResolveReplyField"/>) as <c>text/event-stream</c>: each line is
/// a JSON-encoded chunk, then a trailing JSON object with the full result. We relay chunks to
/// <paramref name="onChunk"/> as they arrive (mirroring <c>RunConversationOperation</c>) and
/// read the reply / conversation id from the final object. The endpoint does not persist the
/// conversation, so the turn is stateless.
/// </summary>
internal sealed class RunDraftAgentTestOperation : IMaintenanceOperation<RunDraftAgentTestOperation.Result>
{
    private readonly AiAgentConfiguration _configuration;
    private readonly string _prompt;
    private readonly IReadOnlyDictionary<string, string>? _parameters;
    private readonly string _streamField;
    private readonly Func<string, Task> _onChunk;

    public RunDraftAgentTestOperation(
        AiAgentConfiguration configuration,
        string prompt,
        IReadOnlyDictionary<string, string>? parameters,
        string streamField,
        Func<string, Task> onChunk)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _prompt = prompt ?? throw new ArgumentNullException(nameof(prompt));
        _parameters = parameters;
        _streamField = streamField ?? throw new ArgumentNullException(nameof(streamField));
        _onChunk = onChunk ?? throw new ArgumentNullException(nameof(onChunk));
    }

    public RavenCommand<Result> GetCommand(DocumentConventions conventions, JsonOperationContext ctx) =>
        new Command(_configuration, _prompt, _parameters, _streamField, _onChunk, conventions);

    public sealed class Result
    {
        // The streamed field's text — used as the chat fallback when nothing streamed.
        public string Reply { get; set; } = "";

        // The full model output object (every declared field), so the caller can surface the
        // whole structured answer rather than only the streamed reply. Null when the turn
        // produced no structured response object.
        public JsonElement? Answer { get; set; }

        public string ConversationId { get; set; } = "";
    }

    private sealed class Command : RavenCommand<Result>
    {
        private readonly AiAgentConfiguration _configuration;
        private readonly string _prompt;
        private readonly IReadOnlyDictionary<string, string>? _parameters;
        private readonly string _replyField;
        private readonly Func<string, Task> _onChunk;
        private readonly DocumentConventions _conventions;

        public Command(
            AiAgentConfiguration configuration,
            string prompt,
            IReadOnlyDictionary<string, string>? parameters,
            string replyField,
            Func<string, Task> onChunk,
            DocumentConventions conventions)
        {
            _configuration = configuration;
            _prompt = prompt;
            _parameters = parameters;
            _replyField = replyField;
            _onChunk = onChunk;
            _conventions = conventions;

            // The server streams the reply property as text/event-stream; read it raw so we can
            // relay chunks as they arrive instead of buffering the whole turn.
            ResponseType = RavenCommandResponseType.Raw;
        }

        // The test endpoint runs the model but writes no cluster state — it is registered as a
        // read endpoint server-side, so route it like one.
        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent/test" +
                  $"?streaming=true&streamPropertyPath={Uri.EscapeDataString(_replyField)}";

            var creationOptions = new DynamicJsonValue();
            if (_parameters is { Count: > 0 })
            {
                var values = new DynamicJsonValue();
                foreach (var (key, value) in _parameters)
                {
                    // Mirror AiConversationParameter so the server binds it like a normal
                    // conversation parameter; SendToModel mirrors the appliance router's
                    // string-parameter convention (always exposed to the model).
                    values[key] = new DynamicJsonValue
                    {
                        ["Value"] = value,
                        ["SendToModel"] = true,
                    };
                }

                creationOptions["Parameters"] = values;
            }

            // Serialize the configuration through the same converter AddOrUpdateAiAgentOperation
            // uses, so the draft binds to AiAgentConfiguration server-side exactly as a
            // provisioned one would.
            var body = new DynamicJsonValue
            {
                ["Configuration"] = _conventions.Serialization.DefaultConverter.ToBlittable(_configuration, ctx),
                ["UserPrompt"] = _prompt,
                ["CreationOptions"] = creationOptions,
            };

            var payload = ctx.ReadObject(body, "ai-agent-test");

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, payload).ConfigureAwait(false), _conventions),
            };
        }

        // Raw streamed response (mirrors RunConversationOperation): each non-"{" line is a
        // JSON-encoded chunk of the reply property; the trailing "{" line is the final result
        // object. We relay chunks to the caller and read the reply / conversation id from the
        // final object (a fallback used when nothing streamed). Parsed with System.Text.Json —
        // RavenDB's blittable sync reader (context.Sync) is internal to the client assembly.
        public override async Task SetResponseRawAsync(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            Result = new Result();

            using var reader = new StreamReader(stream);
            while (true)
            {
                var line = await reader.ReadLineAsync().ConfigureAwait(false);
                if (line is null)
                    break;

                if (line.StartsWith('{'))
                {
                    PopulateResult(line);
                    break;
                }

                if (string.IsNullOrEmpty(line))
                    continue;

                var chunk = JsonSerializer.Deserialize<string>(line);
                if (string.IsNullOrEmpty(chunk) == false)
                    await _onChunk(chunk).ConfigureAwait(false);
            }
        }

        // Not used on the streaming (Raw) path, but RavenCommand requires it.
        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result ??= new Result();
        }

        private void PopulateResult(string finalLine)
        {
            using var doc = JsonDocument.Parse(finalLine);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return;

            if (root.TryGetProperty("ConversationId", out var conversationId) && conversationId.ValueKind == JsonValueKind.String)
                Result.ConversationId = conversationId.GetString() ?? "";

            if (root.TryGetProperty("Response", out var answer) && answer.ValueKind == JsonValueKind.Object)
            {
                // Clone so the element outlives this method's JsonDocument (disposed on return).
                Result.Answer = answer.Clone();
                Result.Reply = ExtractReply(answer);
            }
        }

        // The model answer wraps the reply under the configured reply field; fall back to the
        // first non-empty string property so a custom output shape still surfaces something.
        private string ExtractReply(JsonElement answer)
        {
            if (answer.TryGetProperty(_replyField, out var preferred) &&
                preferred.ValueKind == JsonValueKind.String &&
                preferred.GetString() is { Length: > 0 } text)
            {
                return text;
            }

            foreach (var property in answer.EnumerateObject())
            {
                if (property.Value.ValueKind == JsonValueKind.String &&
                    property.Value.GetString() is { Length: > 0 } value)
                {
                    return value;
                }
            }

            return "";
        }
    }
}
