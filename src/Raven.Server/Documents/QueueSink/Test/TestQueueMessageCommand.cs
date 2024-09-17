using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using System;
using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.Documents.QueueSink.Test;

public sealed class TestQueueMessageCommand : PatchDocumentCommand
{
    private readonly BlittableJsonReaderObject _message;

    public TestQueueMessageCommand(JsonOperationContext context, PatchRequest patch, BlittableJsonReaderObject message) : base(context, Guid.NewGuid().ToString(),
        null, false, (patch, null), (null, null), null, '/', isTest: true, debugMode: true, collectResultsNeeded: true, returnDocument: true)
    {
        _message = message;
    }

    protected override Document GetCurrentDocument(DocumentsOperationContext context, string id, Stopwatch stopwatch, Stopwatch sp, string refreshoriginaldocument)
    {
        return new Document
        {
            Data = _message
        };
    }
}
