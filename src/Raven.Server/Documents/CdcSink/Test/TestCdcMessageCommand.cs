using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink.Test;

public sealed class TestCdcMessageCommand : PatchDocumentCommand
{
    public TestCdcMessageCommand(JsonOperationContext context, PatchRequest patch, BlittableJsonReaderObject rowArgs)
        : base(context, Guid.NewGuid().ToString(),
            null, false, (patch, rowArgs), (null, null), null, '/', isTest: true, debugMode: true, collectResultsNeeded: true, returnDocument: true)
    {
    }

    protected override Document GetCurrentDocument(DocumentsOperationContext context, string id)
    {
        // Return an empty document — the test script modifies it via `this`.
        var empty = new DynamicJsonValue
        {
            ["@metadata"] = new DynamicJsonValue
            {
                ["@collection"] = "TestCdcSink"
            }
        };
        return new Document
        {
            Data = context.ReadObject(empty, "cdc-test-doc")
        };
    }
}
