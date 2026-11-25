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
               - There is an additional field - `extra_content` - carried once per response,
                 on its first tool call, and absent from every other call.
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

            if (toolCallChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Function, out BlittableJsonReaderObject functionChunk) == false)
                throw new InvalidOperationException($"Google tool call '{id}' is missing required field '{ChatCompletionClient.Constants.ResponseFields.Function}'.");

            if (functionChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Name, out string name) == false)
                throw new InvalidOperationException($"Google tool call '{id}' - '{ChatCompletionClient.Constants.ResponseFields.Function}' is missing required field" + 
                                                    $" '{ChatCompletionClient.Constants.ResponseFields.Name}'.");

            if (functionChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Arguments, out string arguments) == false)
                throw new InvalidOperationException($"Google tool call '{id}' - '{ChatCompletionClient.Constants.ResponseFields.Function}' is missing required field" +
                                                    $" '{ChatCompletionClient.Constants.ResponseFields.Arguments}'.");

            _currentCall = new GoogleAiToolCall(id, name, arguments, ReadThoughtSignature(toolCallChunk));
        }

        // Google carries the thought signature once per response, on its first tool call, and omits
        // extra_content entirely on models that do not produce one - requiring it failed every
        // response that held more than one call.
        private static string ReadThoughtSignature(BlittableJsonReaderObject toolCallChunk)
        {
            if (toolCallChunk.TryGetWithoutThrowingOnError("extra_content", out BlittableJsonReaderObject extraContent) == false || extraContent == null)
                return null;

            if (extraContent.TryGetWithoutThrowingOnError("google", out BlittableJsonReaderObject googleContent) == false || googleContent == null)
                return null;

            googleContent.TryGetWithoutThrowingOnError("thought_signature", out string thoughtSignature);
            return thoughtSignature;
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
                var json = new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.ResponseFields.Id] = call.Id,
                    [ChatCompletionClient.Constants.ResponseFields.Type] = ChatCompletionClient.Constants.ResponseFields.Function,
                    [ChatCompletionClient.Constants.ResponseFields.Function] = new DynamicJsonValue
                    {
                        [ChatCompletionClient.Constants.ResponseFields.Name] = call.Name,
                        [ChatCompletionClient.Constants.ResponseFields.Arguments] = call.Arguments
                    }
                };

                if (string.IsNullOrEmpty(call.ThoughtSignature) == false)
                {
                    json["extra_content"] = new DynamicJsonValue
                    {
                        ["google"] = new DynamicJsonValue
                        {
                            ["thought_signature"] = call.ThoughtSignature
                        }
                    };
                }

                toolCalls.Add(json);
            }

            return true;
        }

        public List<AiToolCall> GetAllToolCalls() => _allToolCalls.Values.Cast<AiToolCall>().ToList();
        
        private record GoogleAiToolCall(string Id, string Name, string Arguments, string ThoughtSignature) : AiToolCall(Id, Name, Arguments);
    }

}
