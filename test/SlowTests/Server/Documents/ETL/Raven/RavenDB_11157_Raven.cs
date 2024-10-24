﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_11157_Raven : RavenTestBase
    {
        public RavenDB_11157_Raven(ITestOutputHelper output) : base(output)
        {
        }

        private const string BasicScript = @"
    loadToUsers(this);

function loadCountersOfUsersBehavior(doc, counter)
{
    return true;
}
";

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData("Users", null, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(null, null, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData("Users", BasicScript, DatabaseMode = RavenDatabaseMode.All)]
        public void Should_load_all_counters_when_no_script_is_defined_or_load_counter_behavior_sends_everyting_internal(Options options, string collection, string script)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                if (collection == null)
                    Etl.AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);
                else
                    Etl.AddEtl(src, dest, collection, script: script);

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);

                    var counter = session.CountersFor("users/1").Get("likes");

                    Assert.NotNull(counter);
                    Assert.Equal(1, counter.Value);
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var counter = session.CountersFor("users/1").Get("likes");

                    Assert.NotNull(counter);
                    Assert.Equal(2, counter.Value);
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.Null(user);

                    var counter = session.CountersFor("users/1").Get("likes");

                    Assert.Null(counter);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Should_not_send_counters_metadata_when_using_script(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script: @"this.Name = 'James Doe';
                                       loadToUsers(this);");

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("James Doe", user.Name);

                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Counters));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Should_handle_counters(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script:
                    @"
var counters = this['@metadata']['@counters'];

this.Name = 'James';

// case 1 : doc id will be preserved

var doc = loadToUsers(this);

for (var i = 0; i < counters.length; i++) {
    doc.addCounter(loadCounter(counters[i]));
}

// case 2 : doc id will be generated on the destination side

var person = loadToPeople({ Name: this.Name + ' ' + this.LastName });

person.addCounter(loadCounter('down'));
"
);
                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe",
                        LastName = "Doe",
                    }, "users/1");

                    session.CountersFor("users/1").Increment("up", 20);
                    session.CountersFor("users/1").Increment("down", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertCounters(dest, new[]
                {
                    ("users/1", "up", 20L, false),
                    ("users/1", "down", 10, false),
                    ("users/1/people/", "down", 10, true)
                });

                string personId;

                using (var session = dest.OpenSession())
                {
                    personId = session.Advanced.LoadStartingWith<Person>("users/1/people/")[0].Id;
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Delete("up");
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadata.ContainsKey(Constants.Documents.Metadata.Counters));

                    var counter = session.CountersFor("users/1").Get("up-etl");

                    Assert.Null(counter); // this counter was removed
                }

                AssertCounters(dest, new[]
                {
                    ("users/1", "down", 10L, false),
                    ("users/1/people/", "down", 10, true)
                });

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.Load<User>("users/1"));

                    Assert.Null(session.CountersFor("users/1").Get("up"));
                    Assert.Null(session.CountersFor("users/1").Get("up"));

                    Assert.Empty(session.Advanced.LoadStartingWith<Person>("users/1/people/"));

                    Assert.Null(session.CountersFor(personId).Get("down-etl"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_use_get_counters(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script:
                    @"
var counters = getCounters();

for (var i = 0; i < counters.length; i++) {
    this.LastName = this.LastName + counters[i];
}

var doc = loadToUsers(this);

for (var i = 0; i < counters.length; i++) {
    doc.addCounter(loadCounter(counters[i]));
}
"
);
                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe",
                        LastName = "",
                    }, "users/1");

                    session.CountersFor("users/1").Increment("up");
                    session.CountersFor("users/1").Increment("down", -1);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertCounters(dest, new[]
                {
                    ("users/1", "up", 1L, false),
                    ("users/1", "down", -1, false),
                });

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.Equal("downup", user.LastName);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Should_remove_counter_if_add_counter_gets_null_argument(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script:
                    @"
var doc = loadToUsers(this);
doc.addCounter(loadCounter('likes'));
"
                );
                var etlDone = Etl.WaitForEtlToComplete(src, numOfProcessesToWaitFor: 2);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/1");

                    session.Store(new User()
                    {
                        Name = "Doe"
                    }, "users/2");

                    session.Store(new User()
                    {
                        Name = "Foo"
                    }, "users/3");

                    session.CountersFor("users/1").Increment("up");
                    session.CountersFor("users/2").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    // addCounter(null) should throw transformation error

                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.NotNull(session.Load<User>("users/2"));
                    Assert.NotNull(session.Load<User>("users/3"));
                }

                AssertCounters(dest, new[]
                {
                    ("users/2", "likes", 1L, false)
                });

                etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/2").Delete("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.CountersFor("users/2").Get("likes"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_use_has_counter(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script:
                    @"

var doc = loadToUsers(this);

if (hasCounter('up')) {
  doc.addCounter(loadCounter('up'));
}

if (hasCounter('down')) {
  doc.addCounter(loadCounter('down'));
}
"
                );
                var etlDone = Etl.WaitForEtlToComplete(src, numOfProcessesToWaitFor: 2);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/1");

                    session.CountersFor("users/1").Increment("down", -1);

                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/2");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertCounters(dest, new[]
                {
                    ("users/1", "down", -1L, false)
                });

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.NotNull(session.Load<User>("users/2"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Must_not_send_counters_and_counter_tombstones_from_non_relevant_collections(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script: null);

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = dest.OpenSession())
                {
                    session.Store(new Person(), "people/1");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new Person(), "people/1");

                    session.CountersFor("people/1").Increment("likes");

                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.CountersFor("users/1").Get("likes"));
                    Assert.Null(session.CountersFor("people/1").Get("likes"));
                }

                using (var session = dest.OpenSession())
                {
                    session.CountersFor("people/1").Increment("likes", 15);

                    session.SaveChanges();
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("people/1").Delete("likes");
                    session.CountersFor("users/1").Delete("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.CountersFor("users/1").Get("likes"));
                    Assert.NotNull(session.CountersFor("people/1").Get("likes"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Should_send_counter_even_if_doc_was_updater_later(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1"); // will get higher etag than the counter

                    session.SaveChanges();
                }

                Etl.AddEtl(src, dest, "Users", script: null);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));

                    Assert.NotNull(session.CountersFor("users/1").Get("likes"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Should_send_updated_counter_values(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script: null);

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                etlDone.Reset();

                for (int i = 0; i < 3; i++)
                {
                    using (var session = src.OpenSession())
                    {
                        session.CountersFor("users/1").Increment("likes");

                        session.SaveChanges();
                    }

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var session = dest.OpenSession())
                    {
                        Assert.Equal(i + 2, session.CountersFor("users/1").Get("likes"));
                    }

                    etlDone.Reset();
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public void Should_skip_counter_if_has_lower_etag_than_document(Options options)
        {
            options.ModifyDatabaseRecord += x => 
                x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "2";

            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script: null);

                var etlDone = Etl.WaitForEtlToComplete(src, numOfProcessesToWaitFor: 2);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                etlDone = Etl.WaitForEtlToComplete(src, numOfProcessesToWaitFor: 3);

                using (var session = src.OpenSession())
                {
                    session.Advanced.Defer(
                        new CountersBatchCommandData("users/1", new CounterOperation()
                        {
                            Delta = 1,
                            Type = CounterOperationType.Increment,
                            CounterName = "likes"
                        }),
                        new PutCommandData("users/3", null, new DynamicJsonValue()
                        {
                            ["@metadata"] = new DynamicJsonValue
                            {
                                ["@collection"] = "Users"
                            },
                        }),
                        new PutCommandData("users/4", null, new DynamicJsonValue()
                        {
                            ["@metadata"] = new DynamicJsonValue
                            {
                                ["@collection"] = "Users"
                            },
                        }),
                        new PutCommandData("users/1", null, new DynamicJsonValue()
                        {
                            ["Name"] = "James",
                            ["@metadata"] = new DynamicJsonValue
                            {
                                ["@collection"] = "Users"
                            }
                        }));

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1", includes: i => i.IncludeAllCounters());

                    var value = session.CountersFor("users/1").Get("likes");

                    if (user.Name == "James")
                    {
                        // already managed to etl the document and its counter after doc modification

                        Assert.Equal(2, value);
                    }
                    else
                    {
                        // didn't etl the modified doc yet

                        Assert.Equal(1, value);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData("Users", DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(null, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Should_send_all_counters_on_doc_update(Options options, string collection)
        {
            options.ModifyDatabaseRecord +=
                x => x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "2";

            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");

                    session.CountersFor("users/1").Increment("likes");
                    session.CountersFor("users/2").Increment("likes");

                    session.Store(new User(), "users/3");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                if (collection == null)
                    Etl.AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);
                else
                    Etl.AddEtl(src, dest, "Users", script: null);

                await Etl.AssertEtlReachedDestination(() =>
                {
                    using (var session = dest.OpenSession())
                    {
                        var user = session.Load<User>("users/1");
                        Assert.NotNull(user);
                        long? value = session.CountersFor("users/1").Get("likes");
                        Assert.Equal(1, value);
                    }
                });
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Should_handle_counters_according_to_behavior_defined_in_script(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script:
                    @"
if (this.Age > 20)
{
    loadToUsers({ Name: this.Name + ' ' + this.LastName });
}

function loadCountersOfUsersBehavior(docId, counter)
{
    var user = load(docId);

    if (user.Age > 20 && counter == 'up')
    {
        return true;
    }
}");
                var etlDone = Etl.WaitForEtlToComplete(src);
                
                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Joe",
                        LastName = "Doe",
                        Age = 21
                    }, "users/1");

                    session.CountersFor("users/1").Increment("up", 20);
                    session.CountersFor("users/1").Increment("down", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertCounters(dest, new[]
                {
                    ("users/1", "up", 20L, false),
                });

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Increment("up", 20);
                    session.CountersFor("users/1").Increment("down", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertCounters(dest, new[]
                {
                    ("users/1", "up", 40L, false),
                });

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Delete("up");
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Counters));

                    var counter = session.CountersFor("users/1").Get("up");

                    Assert.Null(counter); // this counter was removed
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Should_not_send_counters_if_load_counters_behavior_isnt_defined(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script:
                    @"
loadToUsers(this);");

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("up", 20);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.Empty(session.CountersFor("users/1").GetAll());
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Increment("up", 20);
                    session.CountersFor("users/1").Increment("down", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Empty(session.CountersFor("users/1").GetAll());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Should_send_all_counters_on_doc_update_if_load_counters_behavior_set(Options options)
        {
            using (var src = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "2"
            }))
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");

                    session.CountersFor("users/1").Increment("likes");
                    session.CountersFor("users/2").Increment("likes");

                    session.Store(new User(), "users/3");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                Etl.AddEtl(src, dest, "Users", script: @"
loadToUsers(this);

function loadCountersOfUsersBehavior(docId, counter)
{
    return true;
}");
                await Etl.AssertEtlReachedDestination(() =>
                {
                    using (var session = dest.OpenSession())
                    {
                        var doc = session.Load<User>("users/1");
                        Assert.NotNull(doc);
                        long? value = session.CountersFor("users/1").Get("likes");
                        Assert.Equal(1, value);
                    }
                });

            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public void Error_if_load_counter_behavior_func_doesnt_match_any_collection_that_script_applies_to()
        {
            var config = new RavenEtlConfiguration
            {
                Name = "test",
                ConnectionStringName = "test",
                Transforms =
                {
                    new Transformation
                    {
                        Name = "test",
                        Collections = {"Users", "Customers"},
                        Script = @"
loadToUsers(this);

function loadCountersOfPeopleBehavior(docId, counter) // note People while script defined for Users and Customers
{
    return true;
}

function loadCountersOfCustomersBehavior(docId, counter) // it's ok
{
    return true;
}
"
                    }
                }
            };

            config.Initialize(new RavenConnectionString() { Database = "Foo", TopologyDiscoveryUrls = new[] { "http://localhost:8080" } });

            config.Validate(out List<string> errors);

            Assert.True(0 < errors.Count);

            Assert.Equal("There is 'loadCountersOfPeopleBehavior' function defined in 'test' script while the processed collections ('Users', 'Customers') doesn't include 'People'. " +
                         "loadCountersOf<CollectionName>Behavior() function is meant to be defined only for counters of docs from collections that " +
                         "are loaded to the same collection on a destination side", errors[0]);
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Load_counters_behavior_function_can_use_other_function_defined_in_script(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script: @"
loadToUsers(this);

function loadAllCounters(){
    return true;
}

function loadCountersOfUsersBehavior(docId, counter)
{
    return loadAllCounters();
}");

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("up", 20);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.Equal(20, session.CountersFor("users/1").Get("up"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Should_override_counter_value(Options options)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script: null);

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("up", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.Equal(10, session.CountersFor("users/1").Get("up"));

                    session.CountersFor("users/1").Increment("up", 1000);

                    session.SaveChanges();

                    Assert.Equal(1010, session.CountersFor("users/1").Get("up"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Increment("up", -10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Equal(0, session.CountersFor("users/1").Get("up"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_define_multiple_load_counter_behavior_functions(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, collections: new[] { "Users", "Employees" }, script:
                    @"

    var collection = this['@metadata']['@collection'];

    if (collection == 'Users')
        loadToUsers(this);
    else if (collection == 'Employees')
        loadToEmployees(this);

    function loadCountersOfUsersBehavior(doc, counter)
    {
        return true;
    }

    function loadCountersOfEmployeesBehavior(doc, counter)
    {
        return true;
    }
");

                var etlDone = Etl.WaitForEtlToComplete(src, numOfProcessesToWaitFor: 2);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        LastName = "Joe"
                    }, "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.Store(new Employee()
                    {
                        LastName = "Joe"
                    }, "employees/1");

                    session.CountersFor("employees/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Equal(1, session.CountersFor("users/1").Get("likes"));
                    Assert.Equal(1, session.CountersFor("employees/1").Get("likes"));
                }

                etlDone = Etl.WaitForEtlToComplete(src, numOfProcessesToWaitFor: 2);

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Increment("likes");
                    session.CountersFor("employees/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Equal(2, session.CountersFor("users/1").Get("likes"));
                    Assert.Equal(2, session.CountersFor("employees/1").Get("likes"));
                }
            }
        }
        internal static void AssertCounters(IDocumentStore store, params (string DocId, string CounterName, long CounterValue, bool LoadUsingStartingWith)[] items)
        {
            using (var session = store.OpenSession())
            {
                foreach (var item in items)
                {
                    var doc = item.LoadUsingStartingWith ? session.Advanced.LoadStartingWith<User>(item.DocId)[0] : session.Load<User>(item.DocId);
                    Assert.NotNull(doc);

                    var metadata = session.Advanced.GetMetadataFor(doc);

                    Assert.True(metadata.ContainsKey(Constants.Documents.Metadata.Counters));

                    var value = session.CountersFor(doc.Id).Get(item.CounterName);

                    Assert.NotNull(value);
                    Assert.Equal(item.CounterValue, value);
                }
            }
        }
    }
}
