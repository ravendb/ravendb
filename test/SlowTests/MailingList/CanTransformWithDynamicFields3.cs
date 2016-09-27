using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using SlowTests.Utils;
using Xunit;

namespace SlowTests.MailingList
{
    public class CanTransformWithDynamicFields : RavenTestBase
    {
        private readonly IDocumentStore _store;

        public CanTransformWithDynamicFields()
        {
            _store = GetDocumentStore();

            // SetUp
            using (var session = _store.OpenSession())
            {
                session.Store(new BaseEntity
                {
                    Id = "entity/1",
                    SupportedLanguages = new List<string> { "en", "pt" }
                });
                session.Store(new Entity
                {
                    Id = "entity/1/en",
                    BaseId = "entity/1",
                    Language = "en",
                    Title = "Hello world"
                });
                session.Store(new Entity
                {
                    Id = "entity/1/pt",
                    BaseId = "entity/1",
                    Language = "pt",
                    Title = "Ole mundo"
                });
                session.SaveChanges();
            }
        }

        private class BaseEntity
        {
            public BaseEntity()
            {
                SupportedLanguages = new List<string>();
            }

            public string Id { get; set; }
            public IEnumerable<string> SupportedLanguages { get; set; }
        }

        private class Entity
        {
            public string Id { get; set; }
            public string BaseId { get; set; }
            public string Title { get; set; }
            public string Language { get; set; }
        }

        private class TranslatedEntities_Map : AbstractIndexCreationTask<BaseEntity, TranslatedEntities_Map.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public Dictionary<string, string> Title { get; set; }
                public object _ { get; set; }
                public IEnumerable<string> SupportedLanguages { get; set; }
            }

            public TranslatedEntities_Map()
            {
                Map = baseEntities =>
                from baseEntity in baseEntities
                let supportedLanguages = baseEntity.SupportedLanguages.Select(x => LoadDocument<Entity>(baseEntity.Id + "/" + x))
                select new Result
                {
                    Id = baseEntity.Id,
                    SupportedLanguages = supportedLanguages.Select(x => x.Language),
                    Title = supportedLanguages.ToDictionary(x => x.Language, entity => entity.Title),
                    _ = supportedLanguages.Select(x => CreateField("Title_" + x.Language, x.Title, true, false))
                };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class TranslatedEntities_MapReduce : AbstractMultiMapIndexCreationTask<TranslatedEntities_MapReduce.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public Dictionary<string, string> Title { get; set; }
                public object _ { get; set; }
                public IEnumerable<string> SupportedLanguages { get; set; }
            }

            public TranslatedEntities_MapReduce()
            {
                AddMap<BaseEntity>(baseEntities =>
                    from baseEntity in baseEntities
                    select new Result
                    {
                        Id = baseEntity.Id,
                        SupportedLanguages = baseEntity.SupportedLanguages,
                        Title = null,
                        _ = null
                    });

                AddMap<Entity>(entities =>
                    from entity in entities
                    select new Result
                    {
                        Id = entity.BaseId,
                        SupportedLanguages = null,
                        Title = new Dictionary<string, string> { { entity.Language, entity.Title } },
                        _ = null
                    });

                Reduce = results =>
                    from result in results
                    group result by result.Id
                        into g
                    select new Result
                    {
                        Id = g.Key,
                        SupportedLanguages = g.First(x => x.SupportedLanguages != null).SupportedLanguages,
                        Title = g.SelectMany(x => x.Title)
                            .ToDictionary(x => x.Key, pair => pair.Value),
                        _ = g.Where(x => x.Title != null)
                            .SelectMany(x => x.Title)
                            .Select(x => CreateField("Title_" + x.Key, x.Value, true, true))
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class GlobalizationTransformer : AbstractTransformerCreationTask<RavenJObject>
        {
            public const string GlobalizationQueryListenerKey = "Language";

            public GlobalizationTransformer()
            {
                TransformResults = results =>
                    from result in results
                    select new
                    {
                        Id = result.Value<string>("Id"),
                        // It works if I get the title from the dictionary, however I want to get directly from the dynamic field
                        //Title = result.Value<IDictionary<string, string>>("Title")[Parameter(GlobalizationQueryListenerKey).Value<string>()],
                        Title = result.Value<string>(string.Join("_", "Title", Parameter(GlobalizationQueryListenerKey).Value<string>()))
                    };
            }
        }

        private class BaseEntityResult
        {
            public string Id { get; set; }
            public string Title { get; set; }
        }

        [Fact]
        public void WillMapPropertiesOnMapIndexes()
        {
            new TranslatedEntities_Map().Execute(_store);
            new GlobalizationTransformer().Execute(_store);

            WaitForIndexing(_store);
            using (var session = _store.OpenSession())
            {
                var results = session.Advanced.DocumentQuery<BaseEntityResult, TranslatedEntities_Map>()
                    .SetResultTransformer(typeof(GlobalizationTransformer).Name)
                    .SetTransformerParameters(new Dictionary<string, RavenJToken>
                        {
                            {GlobalizationTransformer.GlobalizationQueryListenerKey, new RavenJValue("pt")}
                        })
                    .ToList();

                Assert.Equal(1, results.Count);
                Assert.Equal("entity/1", results.First().Id);
                Assert.Equal("Ole mundo", results.First().Title);
                TestHelper.AssertNoIndexErrors(_store);
            }
        }

        [Fact]
        public void WillMapPropertiesOnMapReduceIndexes()
        {
            new TranslatedEntities_MapReduce().Execute(_store);
            new GlobalizationTransformer().Execute(_store);

            WaitForIndexing(_store);

            using (var session = _store.OpenSession())
            {
                var results = session.Advanced.DocumentQuery<BaseEntityResult, TranslatedEntities_MapReduce>()
                    .SetResultTransformer(typeof(GlobalizationTransformer).Name)
                    .SetTransformerParameters(new Dictionary<string, RavenJToken>
                        {
                            {GlobalizationTransformer.GlobalizationQueryListenerKey, new RavenJValue("pt")}
                        })
                    .ToList();

                Assert.Equal(1, results.Count);
                Assert.Equal("entity/1", results.First().Id);
                Assert.Equal("Ole mundo", results.First().Title);
                TestHelper.AssertNoIndexErrors(_store);
            }
        }
    }
}
