// -----------------------------------------------------------------------
//  <copyright file="LoDash.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Database.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class LoDash : RavenTest
    {
        [Fact]
        public void Manual()
        {
            var doc = RavenJObject.FromObject(new Product
            {
                Tags = new string[0],
            });
            var resultJson = new ScriptedJsonPatcher().Apply(doc, new ScriptedPatchRequest
            {
                Script = "this.Tags2 = this.Tags.Map(function(value) { return value; });",
            });
            Assert.Equal(0, resultJson.Value<RavenJArray>("Tags2").Length);
        }

        [Fact]
        public void MapOfEmptyArrayShouldNotAddAnEmtpyItem()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Tags = new string[0],
                    });
                    session.SaveChanges();
                }

                store.DatabaseCommands.Patch("products/1", new ScriptedPatchRequest { Script = "this.Tags2 = this.Tags.Map(function(value) { return value; });" });
                var jsonDocument = store.DatabaseCommands.Get("products/1");
                Assert.Equal("[]", jsonDocument.DataAsJson["Tags2"].ToString());
            }
        }

        private class Product
        {
            public string[] Tags { get; set; }
        }
    }
}