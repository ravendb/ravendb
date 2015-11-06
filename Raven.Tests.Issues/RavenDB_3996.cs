// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3996.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Jint;
using Jint.Native;
using Jint.Runtime;
using Raven.Database.Json;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3996 : RavenTest
    {
        [Fact]
        public void NullStringPropertiesShouldBeConvertedProperly()
        {
            using (var scope = new DefaultScriptedJsonPatcherOperationScope())
            {
                var engine = new Engine();
                var jsObject = engine.Object.Construct(Arguments.Empty);
                jsObject.Put("Test", new JsValue((string)null), true);

                var result = scope.ToRavenJObject(jsObject);

                Assert.Null(result.Value<string>("Test"));
            }
        }
    }
}