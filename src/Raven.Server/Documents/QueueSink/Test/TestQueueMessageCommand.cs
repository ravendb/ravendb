using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using System;
using Sparrow.Json;

namespace Raven.Server.Documents.QueueSink.Test;

public class TestQueueMessageCommand : PatchDocumentCommand
{
    private readonly BlittableJsonReaderObject _message;

    public TestQueueMessageCommand(JsonOperationContext context, PatchRequest patch, BlittableJsonReaderObject message) : base(context, Guid.NewGuid().ToString(),
        null, false, (patch, null), (null, null), null, '/', isTest: true, debugMode: true, collectResultsNeeded: true, returnDocument: true)
    {
        _message = message;
    }

    protected override Document GetCurrentDocument(DocumentsOperationContext context, string id)
    {
        return new Document
        {
            Data = _message
        };
    }
}
