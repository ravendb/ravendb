using System;
using System.Linq;
using FluentAssertions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Database.Linq.PrivateExtensions;
using Raven.Json.Linq;
using Raven.Tests.Bundles.ScriptedIndexResults;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3994 : RavenTest
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults";
        }

        [Fact]
        public void EachDocumentOutputHasItsOwnKey()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new ScriptedIndexResults
                    {
                        Id = ScriptedIndexResults.IdPrefix + new AnimalsPseudoReduce().IndexName,
                        IndexScript = @"",
                        DeleteScript = @"PutDocument('DeleteScriptRan', {})"
                    });
                    s.SaveChanges();
                }
                string docId;
                using (var s = store.OpenSession())
                {
                    var animal = new Animal
                    {
                        Id = "pluto",
                        Name = "Pluto",
                        Type = "Dog"
                    };
                    s.Store(animal);

                    docId = s.Advanced.GetDocumentId(animal);

                    s.SaveChanges();
                }

                new AnimalsPseudoReduce().Execute(store);

                WaitForIndexing(store);
                
                store.DatabaseCommands.Delete(docId, null);

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    s.Load<RavenJObject>("DeleteScriptRan").Should().NotBeNull();
                }
            }
        }


        public class AnimalsPseudoReduce : AbstractMultiMapIndexCreationTask<AnimalsPseudoReduce.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
            }
            public AnimalsPseudoReduce()
            {
                AddMap<Animal>(animals =>
                    from animal in animals
                    select new
                    {
                        animal.Id,
                        animal.Name
                    });

                Reduce = animals => from animal in animals
                    group animal by animal.Id into a
                    select new
                    {
                        Id = a.Single().Id,
                        Name = a.Single().Name
                    };

                Store(a => a.Name, FieldStorage.Yes);
            }
        }
    }
}