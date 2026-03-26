using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.Settings
{
    public class GoogleToolCallState : IToolCallState
    {
        private Dictionary<string, GoogleAiToolCall> _allToolCalls;
        private GoogleAiToolCall _currentCall;

        public GoogleToolCallState()
        {
        }

        public void Merge(BlittableJsonReaderObject toolCallChunk)
        {
            /*
            Expected (Google):
            {
              "id": "call_abc123",
              "type": "function",
              "function": {
                "name": "searchProduct",
                "arguments": "{ \"id\": 1 }"   // full JSON string, not partial
              },
              "extra_content": {
                "google": {
                  "thought_signature": "..."   // optional
                }
              }
            }

            Unlike OpenAI streaming:
               - The full tool call is returned in a single chunk (no deltas).
               - There is no "index" field.
               - All required fields must be present.
               - There is additional field - `extra_content`.
            */

            if (toolCallChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Id, out string id) == false)
            {
                throw new InvalidOperationException($"Google tool call chunk is missing required field '{ChatCompletionClient.Constants.ResponseFields.Id}'.");
            }

            if (_currentCall != null)
            {
                if (id != _currentCall.Id)
                    AddAndReset();
                else
                    return;
            }

            if (toolCallChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Type, out string type) == false)
                throw new InvalidOperationException($"Google tool call '{id}' is missing required field '{ChatCompletionClient.Constants.ResponseFields.Type}'.");
            
            if (toolCallChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Function, out BlittableJsonReaderObject functionChunk) == false)
                throw new InvalidOperationException($"Google tool call '{id}' is missing required field '{ChatCompletionClient.Constants.ResponseFields.Function}'.");

            if (functionChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Name, out string name) == false)
                throw new InvalidOperationException($"Google tool call '{id}' - '{ChatCompletionClient.Constants.ResponseFields.Function}' is missing required field" + 
                                                    $" '{ChatCompletionClient.Constants.ResponseFields.Name}'.");

            if (functionChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Arguments, out string arguments) == false)
                throw new InvalidOperationException($"Google tool call '{id}' - '{ChatCompletionClient.Constants.ResponseFields.Function}' is missing required field" +
                                                    $" '{ChatCompletionClient.Constants.ResponseFields.Arguments}'.");

            if (toolCallChunk.TryGet("extra_content", out BlittableJsonReaderObject extraContent) == false)
                throw new InvalidOperationException($"Google tool call '{id}' - is missing required field 'extra_content'.");
            

            if (extraContent.TryGet("google", out BlittableJsonReaderObject googleContent) == false)
                throw new InvalidOperationException($"Google tool call '{id}' - 'extra_content' is missing required field 'google'.");
            

            if (googleContent.TryGet("thought_signature", out string thoughtSignature) == false)
                throw new InvalidOperationException($"Google tool call '{id}' - 'extra_content.google' is missing required field 'thought_signature'.");

            _currentCall = new GoogleAiToolCall(id, name, arguments, thoughtSignature);
        }

        public void AddAndReset()
        {
            if (_currentCall == null)
                return;

            _allToolCalls ??= new();
            _allToolCalls.TryAdd(_currentCall.Id, _currentCall);
            _currentCall = null;
        }

        public bool TryGetToolCallsForMessage(out DynamicJsonArray toolCalls)
        {
            toolCalls = null;
            if (_allToolCalls == null || _allToolCalls.Count == 0)
                return false;

            toolCalls = new();
            foreach (var (_, call) in _allToolCalls)
            {
                toolCalls.Add(new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.ResponseFields.Id] = call.Id,
                    [ChatCompletionClient.Constants.ResponseFields.Type] = ChatCompletionClient.Constants.ResponseFields.Function,
                    [ChatCompletionClient.Constants.ResponseFields.Function] = new DynamicJsonValue
                    {
                        [ChatCompletionClient.Constants.ResponseFields.Name] = call.Name,
                        [ChatCompletionClient.Constants.ResponseFields.Arguments] = call.Arguments
                    },
                    ["extra_content"] = new DynamicJsonValue
                    {
                        ["google"] = new DynamicJsonValue
                        {
                            ["thought_signature"] = call.ThoughtSignature
                        }
                    }
                });
            }

            return true;
        }

        public List<AiToolCall> GetAllToolCalls() => _allToolCalls.Values.Cast<AiToolCall>().ToList();
        
        private record GoogleAiToolCall(string Id, string Name, string Arguments, string ThoughtSignature) : AiToolCall(Id, Name, Arguments);
    }

}
