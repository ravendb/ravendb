// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3996.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using FastTests;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3996 : RavenTestBase
    {
        [Fact]
        public async Task NullStringPropertiesShouldBeConvertedProperly()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                using (var scope = new PatcherOperationScope(database).Initialize(context))
                {
                    var engine = new Engine();
                    var jsObject = engine.Object.Construct(Arguments.Empty);
                    jsObject.Put("Test", new JsValue((string)null), true);

                    var result = scope.ToBlittable(jsObject);
                    var json = context.ReadObject(result, "test");

                    object value;
                    Assert.True(json.TryGetMember("Test", out value));
                    Assert.Null(value);
                }
            }
        }
    }
}