using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13367 : RavenTestBase
    {
        public RavenDB_13367(ITestOutputHelper output) : base(output)
        {
        }

        public class Entity
        {
            [JsonProperty(PropertyName = "Id")]
            public string Id { get; set; }
            [JsonProperty(PropertyName = "Name")]
            public string Name { get; set; }

            [JsonProperty(PropertyName = "Name-hyphen")]
            public string NameHypen { get; set; }

            [JsonProperty(PropertyName = "Name, hyphen")]
            public string NameComma { get; set; }

            [JsonProperty(PropertyName = "subentity, hyphen")]
            public List<SubEntity> SubEntityComma { get; set; }

            [JsonProperty(PropertyName = "conventional")]
            public List<SubEntity> Conventional { get; set; }

            [JsonProperty(PropertyName = "conv-entional")]
            public List<SubEntity> Conv_entional { get; set; }

            [JsonProperty(PropertyName = "conv_entional")]
            public List<SubEntity> Conv__entional { get; set; }

        }

        public class SubEntity
        {
            [JsonProperty(PropertyName = "Name")]
            public string Name { get; set; }

            [JsonProperty(PropertyName = "Name-hyphen")]
            public string NameHypen { get; set; }

            [JsonProperty(PropertyName = "Name, hyphen")]
            public string NameComma { get; set; }
        }

        public class ConventionalHyphenIndex : AbstractIndexCreationTask<Entity>
        {

            public ConventionalHyphenIndex()
            {
                Map = entities => from e in entities
                                  from subentity in e.Conv_entional // conv-entional -> not valid c# porperty name
                                  select new
                                  {
                                      Name = subentity.NameComma
                                  };
            }
        }

        public class ConventionalUnderScoreIndex : AbstractIndexCreationTask<Entity>
        {

            public ConventionalUnderScoreIndex()
            {
                Map = entities => from e in entities
                                  from subentity in e.Conv__entional // conv_entional -> not valid c# porperty name
                                  select new
                                  {
                                      Name = subentity.Name
                                  };
            }
        }

        public class SubEntityCommaIndex : AbstractIndexCreationTask<Entity>
        {

            public SubEntityCommaIndex()
            {
                Map = entities => from e in entities
                                  from subentity in e.SubEntityComma // subentity, hyphen -> not valid c# porperty name
                                  select new
                                  {
                                      Name = e.Name
                                  };
            }
        }

        [Fact]
        public void CanCreateIndexes()
        {
            using (var store = GetDocumentStore())
            {
                new SubEntityCommaIndex().Execute(store);
                new ConventionalUnderScoreIndex().Execute(store);
                new ConventionalHyphenIndex().Execute(store);
            }
        }


        [Fact]
        public void CanQuery()
        {
            using (var store = GetDocumentStore())
            {
                new ConventionalHyphenIndex().Execute(store);

                using (var s = store.OpenSession())
                {
                    var e = new Entity
                    {
                        Conv_entional = new List<SubEntity>
                        {
                            new SubEntity { NameComma = "Oren" }
                        }
                    };
                    s.Store(e);
                    s.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var r = s.Query<SubEntity, ConventionalHyphenIndex>()
                        .Where(x => x.Name == "Oren")
                        .OfType<Entity>()
                        .ToList();
                    WaitForUserToContinueTheTest(store);
                    Assert.NotEmpty(r);
                }
            }
        }

    }
}
