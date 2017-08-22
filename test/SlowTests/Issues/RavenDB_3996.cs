// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3996.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using FastTests;
using Jurassic;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3996 : RavenTestBase
    {
        [Fact]
        public void NullStringPropertiesShouldBeConvertedProperly()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var empty = context.ReadObject(new DynamicJsonValue(), "empty");
                var engine = new ScriptEngine();
                var jsObject = new BlittableObjectInstance(engine,empty, "n");
                jsObject.SetPropertyValue("Test", (string)null, true);

                var json = new ScriptRunnerResult(jsObject).Translate<BlittableJsonReaderObject>(context);
                object value;
                Assert.True(json.TryGetMember("Test", out value));
                Assert.Null(value);
            }
        }
    }
}
