// -----------------------------------------------------------------------
//  <copyright file="PatchingWithDefault.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class PatchingWithDefault : RavenTestBase
    {
        [Fact]
        public void PatchRequestShouldCreateDocIfNotExists()
        {
            using (var store = NewDocumentStore())
            {
                var patchExisting = new ScriptedPatchRequest
                {
                    Script = "this.Counter++;",
                };
                var patchDefault = new ScriptedPatchRequest
                {
                    Script = "this.Counter=100;",
                };
                var docId = "Docs/1";

                store.DatabaseCommands.Patch(docId, patchExisting, patchDefault, new RavenJObject());

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Load<TestDoc>(docId));
                }
            }
        }

        public class TestDoc
        {
            public int Counter { get; set; }
        }
    }
}