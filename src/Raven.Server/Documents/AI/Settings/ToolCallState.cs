using System.Collections.Generic;
using System.Text;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.Settings
{
    public class ToolCallState : IToolCallState
    {
        private StringBuilder _id;
        private StringBuilder _type;
        private StringBuilder _name;
        private StringBuilder _arguments;

        private int _toolCallIndex;

        private List<AiToolCall> _allToolCalls;

        public ToolCallState()
        {
            _toolCallIndex = -1;
        }

        public void Merge(BlittableJsonReaderObject toolCallChunk)
        {
            /*
            Expected json:
            {
              "index": 0,
              "id": "call_abc123",
              "type": "function", 
              "function": {
                "name": "searchProduct",   
                "arguments": "{ \"id\": 1"  
              }
            }
            */

            if (toolCallChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Index, out int index) == false)
                return;

            if (index != _toolCallIndex)
            {
                AddAndReset();
                _toolCallIndex = index;
            }


            if (toolCallChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Id, out string id))
            {
                _id.Append(id);
            }

            if (toolCallChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Type, out string type))
            {
                _type.Append(type);
            }

            if (toolCallChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Function, out BlittableJsonReaderObject functionChunk))
            {
                if (functionChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Name, out string nameChunk))
                {
                    _name.Append(nameChunk);
                }

                if (functionChunk.TryGet(ChatCompletionClient.Constants.ResponseFields.Arguments, out string argsChunk))
                {
                    _arguments.Append(argsChunk);
                }
            }
        }

        public void AddAndReset()
        {
            if (_toolCallIndex == -1)
            {
                _id ??= new();
                _type ??= new();
                _name ??= new();
                _arguments ??= new();

                return;
            }

            _allToolCalls ??= [];
            _allToolCalls.Add(new AiToolCall(_id.ToString(), _name.ToString(), _arguments.ToString()));


            _toolCallIndex = -1;
            _id.Clear();
            _type.Clear();
            _name.Clear();
            _arguments.Clear();
        }

        public bool TryGetToolCallsForMessage(out DynamicJsonArray toolCalls)
        {
            toolCalls = null;
            if (_allToolCalls == null || _allToolCalls.Count == 0)
                return false;

            toolCalls = new();
            foreach (var call in _allToolCalls)
            {
                toolCalls.Add(new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.ResponseFields.Id] = call.Id,
                    [ChatCompletionClient.Constants.ResponseFields.Type] = ChatCompletionClient.Constants.ResponseFields.Function,
                    [ChatCompletionClient.Constants.ResponseFields.Function] = new DynamicJsonValue
                    {
                        [ChatCompletionClient.Constants.ResponseFields.Name] = call.Name,
                        [ChatCompletionClient.Constants.ResponseFields.Arguments] = call.Arguments
                    }
                });
            }

            return true;
        }

        public List<AiToolCall> GetAllToolCalls() => _allToolCalls;
    }
}
