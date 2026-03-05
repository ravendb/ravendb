using System;
using System.Collections.Generic;
using System.Text;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.Settings
{
    public interface IToolCallState
    {
        public void Merge(BlittableJsonReaderObject toolCallChunk);
        public void AddAndReset();
        public bool TryGetToolCallsForMessage(out DynamicJsonArray toolCalls);
        public List<AiToolCall> GetAllToolCalls();
    }
}
