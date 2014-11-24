// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1557.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1557 : RavenTest
    {

        public class User
        {
            public string Name { get; set; }
            public string Id { get; set; }
        }

        [Fact]
        public async Task ShouldProperlyPutDocument()
        {
            using (var documentStore = NewRemoteDocumentStore())
            {
                await documentStore.AsyncDatabaseCommands.PutAsync("user/1", null, RavenJObject.FromObject(new User
                {
                    Name = "ayende"
                }), new RavenJObject());

                var doc = await documentStore.AsyncDatabaseCommands.GetAsync("user/1");
                Assert.True(doc.DataAsJson.ContainsKey("Name"));
            }
        }
    }
}