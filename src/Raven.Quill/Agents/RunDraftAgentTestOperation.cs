using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Quill.Contracts;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Quill.Agents;

internal sealed class RunDraftAgentTestOperation : IMaintenanceOperation<RunDraftAgentTestOperation.Result>
{
    private readonly AiAgentConfiguration _configuration;
    private readonly string _prompt;
    private readonly IReadOnlyDictionary<string, SetupTryParameter>? _parameters;
    private readonly string _streamField;
    private readonly Func<string, Task> _onChunk;
    private readonly CancellationToken _token;

    public RunDraftAgentTestOperation(
        AiAgentConfiguration configuration,
        string prompt,
        IReadOnlyDictionary<string, SetupTryParameter>? parameters,
        string streamField,
        Func<string, Task> onChunk,
        CancellationToken token)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _prompt = prompt ?? throw new ArgumentNullException(nameof(prompt));
        _parameters = parameters;
        _streamField = streamField ?? throw new ArgumentNullException(nameof(streamField));
        _onChunk = onChunk ?? throw new ArgumentNullException(nameof(onChunk));
        _token = token;
    }

    public RavenCommand<Result> GetCommand(DocumentConventions conventions, JsonOperationContext ctx) =>
        new Command(_configuration, _prompt, _parameters, _streamField, _onChunk, conventions, _token);

    public sealed class Result
    {
        public string Reply { get; set; } = "";

        public JsonElement? Answer { get; set; }

        public IReadOnlyList<AgentQueryToolCall> ToolCalls { get; set; } = [];

        public string ConversationId { get; set; } = "";
    }

    private sealed class Command : RavenCommand<Result>
    {
        private readonly AiAgentConfiguration _configuration;
        private readonly string _prompt;
        private readonly IReadOnlyDictionary<string, SetupTryParameter>? _parameters;
        private readonly string _replyField;
        private readonly Func<string, Task> _onChunk;
        private readonly DocumentConventions _conventions;
        private readonly CancellationToken _token;

        public Command(
            AiAgentConfiguration configuration,
            string prompt,
            IReadOnlyDictionary<string, SetupTryParameter>? parameters,
            string replyField,
            Func<string, Task> onChunk,
            DocumentConventions conventions,
            CancellationToken token)
        {
            _configuration = configuration;
            _prompt = prompt;
            _parameters = parameters;
            _replyField = replyField;
            _onChunk = onChunk;
            _conventions = conventions;
            _token = token;

            ResponseType = RavenCommandResponseType.Raw;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent/test" +
                  $"?streaming=true&streamPropertyPath={Uri.EscapeDataString(_replyField)}";

            var creationOptions = new DynamicJsonValue();
            if (_parameters is { Count: > 0 })
            {
                var values = new DynamicJsonValue();
                foreach (var (key, parameter) in _parameters)
                {
                    values[key] = new DynamicJsonValue
                    {
                        ["Value"] = AgentTestParameterValue.Convert(parameter?.Value),
                        ["SendToModel"] = parameter?.SendToModel ?? true,
                    };
                }

                creationOptions["Parameters"] = values;
            }

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

        public override async Task SetResponseRawAsync(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            Result = new Result();

            using var reader = new StreamReader(stream);
            while (true)
            {
                var line = await reader.ReadLineAsync(_token).ConfigureAwait(false);
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
                Result.Answer = answer.Clone();
                Result.Reply = ExtractReply(answer);
            }

            Result.ToolCalls = AgentTestTranscript.ExtractQueryToolCalls(root, _configuration);
        }

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
