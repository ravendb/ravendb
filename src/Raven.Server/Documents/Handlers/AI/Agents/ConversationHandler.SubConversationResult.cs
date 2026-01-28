using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Operations.AI.Agents;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    public partial class ConversationHandler
    {
        private sealed class SubConversationResult
        {
            public IDisposable Disposable { get; }
            public List<BlittableJsonReaderObject> Messages { get; } = new();
            public List<string> OpenToolCallsToRemove { get; } = new();
            public Dictionary<string, AiAgentActionRequest> ChildUserCalls { get; } = new();
            public int ToolsIterations { get; set; }

            public SubConversationResult(IDisposable disposable)
            {
                Disposable = disposable;
            }
        }

    }
}
