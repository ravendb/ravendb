// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3996.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3996 : RavenTestBase
    {
        [Fact]
        public void NullStringPropertiesShouldBeConvertedProperly()
        {
            using (var context = new DocumentsOperationContext(null, 1024, 1024, LowMemoryFlag.None))
            {
                var empty = context.ReadObject(new DynamicJsonValue(), "empty");
                var scriptRunner = new ScriptRunner(null, false);
                scriptRunner.AddScript("function ReturnSelf(me){return me;}");
                using (scriptRunner.GetRunner(out var run))
                {
                    var result = run.Run(context, "ReturnSelf", new object[]{empty});
                    var jsObject = result.Value as BlittableObjectInstance;
                    Assert.NotNull(jsObject);

                    jsObject.SetPropertyValue("Test", (string)null, true);
                    var json = new ScriptRunnerResult(null,jsObject).Translate<BlittableJsonReaderObject>(context);
                    object value;
                    Assert.True(json.TryGetMember("Test", out value));
                    Assert.Null(value);
                }
            }
        }
    }
}
