// -----------------------------------------------------------------------
//  <copyright file="ScriptedIndexResultsTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.ScriptedIndexResults
{
    public class ScriptedIndexResultsTest : RavenTest
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults";
        }

        [Fact]
        public void CanUpdateValueOnDocument()
        {
            using (var store = NewDocumentStore())
            {
                new Animals_Stats_With_Scripts().Execute(store);
                using (var s = store.OpenSession())
                {
                    s.Store(new Animal
                    {
                        Name = "Arava",
                        Type = "Dog"
                    });
                    s.Store(new Animal
                    {
                        Name = "Oscar",
                        Type = "Dog"
                    });

                    s.Store(new AnimalType
                    {
                        Id = "AnimalTypes/Dog",
                        Description = "Man's Best Friend"
                    });

                    s.SaveChanges();
                }

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
                    Assert.Equal(2, animalType.Count);
                }
            }
        }

        [Fact]
        public void CanUpdateValueOnDocumentWhenItemIsRemoved()
        {
            using (var store = NewDocumentStore())
            {
                new Animals_Stats_With_Scripts().Execute(store);
                using (var s = store.OpenSession())
                {
                    s.Store(new Animal
                    {
                        Name = "Arava",
                        Type = "Dog"
                    });
                    s.Store(new Animal
                    {
                        Name = "Oscar",
                        Type = "Dog"
                    });

                    s.Store(new AnimalType
                    {
                        Id = "AnimalTypes/Dog",
                        Description = "Man's Best Friend"
                    });

                    s.SaveChanges();
                }

                WaitForIndexing(store);

                store.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery
                {
                    Query = "Tag:Animals"
                }).WaitForCompletion();
                
                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
                    Assert.Equal(0, animalType.Count);
                }
            }
        }
    }
}
