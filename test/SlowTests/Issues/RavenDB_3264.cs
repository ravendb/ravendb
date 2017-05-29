// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3264.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3264 : RavenLowLevelTestBase
    {
        [Fact]
        public void PatcherCanOutputObjectsCorrectly()
        {
            using (var database = CreateDocumentDatabase())
            {
                const string script = @"output(undefined);
                                output(true);
                                output(2);
                                output(2.5);
                                output('string');
                                output(null);
                                output([2, 'c']);
                                output({'a': 'c', 'f': { 'x' : 2}});"
                    ;

                var patch = new PatchRequest
                {
                    Script = script
                };

                DocumentsOperationContext context;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    var document = new Document
                    {
                        Id = context.GetLazyString("keys/1"),
                        Data = context.ReadObject(new DynamicJsonValue(), "keys/1")
                    };

                    var patcher = new DocumentPatcher(database);
                    var result = patcher.Apply(context, document, patch, debugMode: true);
                    dynamic debug = new DynamicBlittableJson(result.Debug);
                    dynamic array = (DynamicArray)debug.Info;

                    Assert.Equal(8 + 1, array.Count); // +1 because of statements executed
                    Assert.Equal("undefined", array[0]);
                    Assert.Equal("True", array[1]);
                    Assert.Equal("2", array[2]);
                    Assert.Equal("2.5", array[3]);
                    Assert.Equal("string", array[4]);
                    Assert.Equal("null", array[5]);
                    Assert.Equal("[2,\"c\"]", array[6]);
                    Assert.Equal("{\"a\":\"c\",\"f\":{\"x\":2}}", array[7]);
                }
            }
        }
    }
}
