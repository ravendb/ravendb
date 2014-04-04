// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1601.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Database.Bundles.ScriptedIndexResults;
using Raven.Database.Json;
using Raven.Json.Linq;
using Raven.Tests.Bundles.ScriptedIndexResults;
using Raven.Tests.Common;

using Xunit;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Tests.Issues
{
    public class RavenDB_1601 : RavenTest
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults";
        }

        public class Developer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public List<Skill> Skills { get; set; }
        }

        public class SkillStat
        {
            public string Id { get; set; }
            public int Count { get; set; }
        }

        public class Skill
        {
            public Skill()
            {
            }
            public Skill(string name)
            {
                Name = name;
            }

            public string Name { get; set; }
        }

        public class OpCounter
        {
            public int Index { get; set; }
            public int Deletes { get; set; }
        }

        private void VerifyOperationsCount(IDocumentSession session, int expectedMinIndexInvocations, int expectedMinDeleteInvocations)
        {
            var opCounter = session.Load<RavenJObject>("opCounter");
            Assert.True(expectedMinIndexInvocations <= opCounter.Value<int>("Index"));
            Assert.True(expectedMinDeleteInvocations <= opCounter.Value<int>("Deletes"));
        }

        [Fact]
        public void CanUpdateRemoveValuesOnDocumentInSimpleIndexMultipleValues()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new ScriptedIndexResults
                    {
                        Id = ScriptedIndexResults.IdPrefix + new Developers_Skills().IndexName,
                        IndexScript = @"
var docId = 'Skills/'+ this.Skill;
var type = LoadDocument(docId) || {};
type.Count++;
var opCounterId = 'opCounter';
var opCounter = LoadDocument(opCounterId) || {};
opCounter.Index++;
PutDocument(opCounterId, opCounter);
PutDocument(docId, type);",
                        DeleteScript = @"
var docId = 'Skills/'+ this.Skill;
var type = LoadDocument(docId);
if(type == null)
	return;
var opCounterId = 'opCounter';
var opCounter = LoadDocument(opCounterId) || {};
opCounter.Deletes++;
PutDocument(opCounterId, opCounter);
type.Count--;
PutDocument(docId, type);
"
                    });
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    s.Store(new Developer
                    {
                        Name = "marcin",
                        Skills = new List<Skill>
                        {
                            new Skill("java"),
                            new Skill("NET")
                        }
                    });
                    s.Store(new Developer
                    {
                        Name = "john",
                        Skills = new List<Skill>
                        {
                            new Skill("NET")
                        }
                    });

                    s.Store(new OpCounter(), "opCounter");

                    s.Store(new SkillStat
                    {
                        Id = "Skills/NET",
                        Count = 0
                    });

                    s.Store(new SkillStat
                    {
                        Id = "Skills/Java",
                        Count = 0
                    });

                    s.SaveChanges();
                }

                new Developers_Skills().Execute(store);

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    VerifyOperationsCount(s, 3, 0);
                    Assert.Equal(2, s.Load<SkillStat>("Skills/NET").Count);
                    Assert.Equal(1, s.Load<SkillStat>("Skills/Java").Count);
                }


                store.DatabaseCommands.Delete("developers/1", null);

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    VerifyOperationsCount(s, 3, 2);
                    Assert.Equal(1, s.Load<SkillStat>("Skills/NET").Count);
                    Assert.Equal(0, s.Load<SkillStat>("Skills/Java").Count);
                }
            }            
        }

        [Fact]
        public void CanUpdateRemoveValuesOnDocumentInSimpleIndex()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new ScriptedIndexResults
                    {
                        Id = ScriptedIndexResults.IdPrefix + new Animals_Simple().IndexName,
                        IndexScript = @"
var docId = 'AnimalTypes/'+ this.Type;
var type = LoadDocument(docId) || {};
type.Count++;
var opCounterId = 'opCounter';
var opCounter = LoadDocument(opCounterId) || {};
opCounter.Index++;
PutDocument(opCounterId, opCounter);
PutDocument(docId, type);",
                        DeleteScript = @"
var docId = 'AnimalTypes/'+ this.Type;
var type = LoadDocument(docId);
if(type == null)
	return;
var opCounterId = 'opCounter';
var opCounter = LoadDocument(opCounterId) || {};
opCounter.Deletes++;
PutDocument(opCounterId, opCounter);
type.Count--;
PutDocument(docId, type);
"
                    });
                    s.SaveChanges();
                }
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
                    s.Store(new Animal
                    {
                        Name = "Pluto",
                        Type = "Dog"
                    });

                    s.Store(new OpCounter(), "opCounter");

                    s.Store(new AnimalType
                    {
                        Id = "AnimalTypes/Dog",
                        Description = "Man's Best Friend"
                    });

                    s.SaveChanges();
                }

                new Animals_Simple().Execute(store);

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
                    Assert.Equal(3, animalType.Count);
                    VerifyOperationsCount(s, 3, 0);
                }

                using (var s = store.OpenSession())
                {
                    var pluto = s.Query<Animal>().Single(a => a.Name == "Pluto");
                    s.Delete(pluto);
                    s.SaveChanges();
                }

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
                    Assert.Equal(2, animalType.Count);
                    VerifyOperationsCount(s, 3, 1);
                }

                using (var s = store.OpenSession())
                {
                    var oscar = s.Query<Animal>().Single(a => a.Name == "Oscar");
                    s.Delete(oscar);
                    s.Store(new Animal
                    {
                        Name = "Rex",
                        Type = "Dog"
                    });
                    s.SaveChanges();
                }

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
                    Assert.Equal(2, animalType.Count);
                    VerifyOperationsCount(s, 4, 2);
                }
            }
        }

        public class Animals_Simple : AbstractIndexCreationTask<Animal, Animals_Simple.Result>
        {
            public class Result
            {
                public string Type { get; set; }
            }
            public Animals_Simple()
            {
                Map = animals =>
                      from animal in animals
                      select new
                      {
                          animal.Type
                      };
                Store(a => a.Type, FieldStorage.Yes);
            }
        }

        public class Developers_Skills : AbstractIndexCreationTask<Developer, Developers_Skills.Result>
        {
            public class Result
            {
                public string Skill { get; set; }
            }

            public Developers_Skills()
            {
                Map = developers => from developer in developers from skill in developer.Skills select new Result { Skill = skill.Name };
                Store(s => s.Skill, FieldStorage.Yes);
            }
        }

        [Fact]
        public void CanUpdateValueOnDocumentInMapReduce()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new ScriptedIndexResults
                    {
                        Id = ScriptedIndexResults.IdPrefix + new Animals_Stats().IndexName,
                        IndexScript = @"
var docId = 'AnimalTypes/'+ key;
var type = LoadDocument(docId) || {};
type.Count = this.Count;
var opCounterId = 'opCounter';
var opCounter = LoadDocument(opCounterId) || {};
opCounter.Index++;
PutDocument(opCounterId, opCounter);
PutDocument(docId, type);",
                        DeleteScript = @"
var docId = 'AnimalTypes/'+ key;
var type = LoadDocument(docId);
if(type == null)
	return;
var opCounterId = 'opCounter';
var opCounter = LoadDocument(opCounterId) || {};
opCounter.Deletes++;
PutDocument(opCounterId, opCounter);
type.Count = 0;
PutDocument(docId, type);
"
                    });
                    s.SaveChanges();
                }
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

                    s.Store(new OpCounter(), "opCounter");

                    s.Store(new AnimalType
                    {
                        Id = "AnimalTypes/Dog",
                        Description = "Man's Best Friend"
                    });

                    s.SaveChanges();
                }

                new Animals_Stats().Execute(store);

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    // one bucket (dog) => one insert
                    VerifyOperationsCount(s, 1, 0);
                    var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
                    Assert.Equal(2, animalType.Count);
                }

                using (var s = store.OpenSession())
                {
                    s.Store(new Animal
                    {
                        Name = "Rex",
                        Type = "Dog"
                    });

                    s.SaveChanges();
                }

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    VerifyOperationsCount(s, 2, 1); 
                    var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
                    Assert.Equal(3, animalType.Count);
                }

                using (var s = store.OpenSession())
                {
                    s.Store(new Animal
                    {
                        Name = "Ara",
                        Type = "Parrot"
                    });

                    s.Store(new Animal
                    {
                        Name = "Pluto",
                        Type = "Dog"
                    });

                    s.SaveChanges();
                }

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    VerifyOperationsCount(s, 4, 2);
                    var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
                    Assert.Equal(4, animalType.Count);
                    var parrotType = s.Load<AnimalType>("AnimalTypes/Parrot");
                    Assert.Equal(1, parrotType.Count);
                }

                using (var s = store.OpenSession())
                {
                    var dogs = s.Query<Animal>().Where(a => a.Type == "Dog");
                    foreach (var dog in dogs)
                    {
                        s.Delete(dog);
                    }
                    s.SaveChanges();
                }

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    // one bucket (dog) => one insert
                    VerifyOperationsCount(s, 4, 3);
                    var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
                    Assert.Equal(0, animalType.Count);
                    var parrotType = s.Load<AnimalType>("AnimalTypes/Parrot");
                    Assert.Equal(1, parrotType.Count);
                }



            }
        }

        [Fact]
        public void CanLoadPutDocumentsMultipleTimesInPatcher()
        {

            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new OpCounter(), "opCounter");
                    s.SaveChanges();
                }
                var patcher =
                    new ScriptedIndexResultsIndexTrigger.Batcher.ScriptedIndexResultsJsonPatcher(
                        store.DocumentDatabase, new HashSet<string> { "dogs" });


                patcher.Apply(new RavenJObject(), new ScriptedPatchRequest
                {
                    Script =
@"var opCounterId = 'opCounter';
var opCounter = LoadDocument(opCounterId) || {};
opCounter.Index++;
PutDocument(opCounterId, opCounter);
opCounter = LoadDocument(opCounterId)
opCounter.Deletes++;
PutDocument(opCounterId, opCounter);
"
                });

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    foreach (var operation in patcher.GetOperations())
                    {
                        switch (operation.Type)
                        {
                            case ScriptedJsonPatcher.OperationType.Put:
                                store.DocumentDatabase.Documents.Put(operation.Document.Key, operation.Document.Etag, operation.Document.DataAsJson,
                                             operation.Document.Metadata, null);
                                break;
                            case ScriptedJsonPatcher.OperationType.Delete:
                                store.DocumentDatabase.Documents.Delete(operation.DocumentKey, null, null);
                                break;
                            default:
                                throw new ArgumentOutOfRangeException("operation.Type");
                        }
                    }
                });

                using (var s = store.OpenSession())
                {
                    var opCounter = s.Load<OpCounter>("opCounter");
                    Assert.Equal(1, opCounter.Deletes);
                    Assert.Equal(1, opCounter.Index);
                }
            }
        } 

    }
}