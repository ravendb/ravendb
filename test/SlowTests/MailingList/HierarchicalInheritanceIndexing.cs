using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class HierarchicalInheritanceIndexing : RavenTestBase
    {
        public HierarchicalInheritanceIndexing(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateIndex()
        {
            var rootId = Guid.NewGuid().ToString();
            var childId = Guid.NewGuid().ToString();
            var grandChildId = Guid.NewGuid().ToString();
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    for (int i = 0; i < 20; i++)
                    {
                        var ex = new Example
                        {
                            OwnerId = rootId,
                            Name = string.Format("Example_{0}", i),
                            Description = "Ex Description"
                        };

                        var child = new ExampleOverride
                        {
                            OwnerId = childId,
                            OverriddenValues = new Dictionary<string, object>
                            {
                                {"Name", string.Format("Child_{0}", i)}
                            }
                        };

                        ex.Overrides.Add(child);

                        var grandChild = new ExampleOverride
                        {
                            OwnerId = grandChildId,
                            OverriddenValues = new Dictionary<string, object>
                            {
                                {"Name", string.Format("GrandChild_{0}", i)}
                            }
                        };

                        child.Overrides.Add(grandChild);

                        session.Store(ex);
                    }
                    session.SaveChanges();
                }

                new ExampleIndexCreationTask().Execute(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var examples =
                        session.Query<ExampleProjection, ExampleIndexCreationTask>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ProjectInto<ExampleProjection>()
                        .Take(1024)
                        .ToList();

                    Assert.NotEmpty(examples);
                }
            }
        }

        [JsonObject(IsReference = true)]
        private class Example
        {
            public Example()
            {
                Overrides = new List<ExampleOverride>();
            }
            public string Id { get; set; }
            public string OwnerId { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public List<ExampleOverride> Overrides { get; set; }
        }

        [JsonObject(IsReference = true)]
        private class ExampleOverride
        {
            public ExampleOverride()
            {
                OverriddenValues = new Dictionary<string, object>();
                Overrides = new List<ExampleOverride>();
            }
            public string OwnerId { get; set; }
            public ExampleOverride Parent { get; set; }
            public Dictionary<string, object> OverriddenValues { get; set; }
            public List<ExampleOverride> Overrides { get; set; }
        }

        private class ExampleProjection
        {
            public string Id { get; set; }
            public Guid OwnerId { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
        }

        private class ExampleIndexCreationTask : AbstractMultiMapIndexCreationTask<ExampleProjection>
        {
            public ExampleIndexCreationTask()
            {
                AddMap<Example>(
                    examples => from ex in examples
                                select new { ex.Id, ex.OwnerId, ex.Name, ex.Description }
                    );

                AddMap<Example>(
                    examples => from ex in examples
                                from ov in ex.Overrides
                                let ancestry = Recurse(ov, x => x.Parent)
                                select
                                    new
                                    {
                                        ex.Id,
                                        ov.OwnerId,
                                        Name = ov.OverriddenValues["Name"] ?? (ancestry.Any(x => x.OverriddenValues["Name"] != null)
                                                                                    ? ancestry.First(x => x.OverriddenValues["Name"] != null).OverriddenValues["Name"]
                                                                                    : ex.Name),
                                        Description = ov.OverriddenValues["Description"] ?? (ancestry.Any(x => x.OverriddenValues["Description"] != null)
                                                                                     ? ancestry.First(x => x.OverriddenValues["Description"] != null).OverriddenValues["Description"]
                                                                                     : ex.Description)
                                    });
            }
        }
    }
}
