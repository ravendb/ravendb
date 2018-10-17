using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9995 : RavenTestBase
    {
        [Fact]
        public void CanQueryOnArrays()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var djv = new DynamicJsonValue
                    {
                        ["TagsAsSlugs"] = new DynamicJsonValue
                        {
                            ["$type"] = "RaccoonBlog.Web.Models.Post+<get_TagsAsSlugs>d__70, RaccoonBlog.Web",
                            ["$values"] = new DynamicJsonArray(new[] { "raven", "architecture", "design" })
                        }
                    };

                    var json = commands.Context.ReadObject(djv, "posts/1");

                    commands.Put("posts/1", null, json, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Posts"}
                    });
                }

                using (var session = store.OpenSession())
                {
                    var items = session
                        .Advanced
                        .RawQuery<dynamic>("from Posts where TagsAsSlugs = 'architecture'")
                        .ToList();
                    
                    Assert.Equal(1, items.Count);

                    items = session
                        .Advanced
                        .RawQuery<dynamic>("from Posts where TagsAsSlugs = 'architecture2'")
                        .ToList();

                    Assert.Equal(0, items.Count);
                }
            }
        }
    }
}
