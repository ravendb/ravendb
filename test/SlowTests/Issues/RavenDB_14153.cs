using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14153 : RavenTestBase
    {
        public RavenDB_14153(ITestOutputHelper output) : base(output)
        {
        }
        
        private class Team
        {
            public List<string> Captains { get; set; }
        }

        [Fact]
        public void PatchingShouldSentListArgumentAsJsonArray()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Team()
                    {
                        Captains = new List<string>()
                    }, "teams/1");

                    session.SaveChanges();
                }

                var status = store.Operations.Send(new PatchOperation("teams/1", null, new PatchRequest()
                {
                    Script = "this.Captains = args.Captains",
                    Values =
                    {
                        {"Captains", new List<string>(){ "a", "b", "c"}}
                    }
                }));

                using (var commands = store.Commands())
                {
                    var entity = commands.Get("teams/1");

                    Assert.True(entity.BlittableJson.TryGet("Captains", out BlittableJsonReaderArray captains));

                    Assert.Equal(3, captains.Length);
                    Assert.Equal("a", captains[0].ToString());
                    Assert.Equal("b", captains[1].ToString());
                    Assert.Equal("c", captains[2].ToString());
                }
            }
        }
    }
}
