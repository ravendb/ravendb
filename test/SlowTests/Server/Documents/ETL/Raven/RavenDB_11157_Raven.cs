using System;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_11157_Raven : EtlTestBase
    {
        [Theory]
        [InlineData("Users")]
        [InlineData(null)]
        public void Should_load_all_counters_when_no_script_is_defined(string collection)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                if (collection == null)
                    AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);
                else
                    AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

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

                    var counter = session.CountersFor("users/1").Get( "likes");

                    Assert.Null(counter);
                }
            }
        }

        [Fact]
        public void Should_not_send_counters_metadata_when_using_script()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: @"this.Name = 'James Doe';
                                       loadToUsers(this);");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");

                    session.CountersFor("users/1").Increment( "likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("James Doe", user.Name);

                    var metatata = session.Advanced.GetMetadataFor(user);

                    Assert.False(metatata.ContainsKey(Constants.Documents.Metadata.Counters));
                }
            }
        }


        [Fact]
        public void Should_handle_counters()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
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
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

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

                    var metatata = session.Advanced.GetMetadataFor(user);

                    Assert.True(metatata.ContainsKey(Constants.Documents.Metadata.Counters));

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

        private void AssertCounters(IDocumentStore store, params (string DocId, string CounterName, long CounterValue, bool LoadUsingStartingWith)[] items)
        {
            using (var session = store.OpenSession())
            {
                foreach (var item in items)
                {
                    var doc = item.LoadUsingStartingWith ? session.Advanced.LoadStartingWith<User>(item.DocId)[0] : session.Load<User>(item.DocId);
                    Assert.NotNull(doc);

                    var metatata = session.Advanced.GetMetadataFor(doc);

                    Assert.True(metatata.ContainsKey(Constants.Documents.Metadata.Counters));
                    
                    var value = session.CountersFor(doc.Id).Get(item.CounterName);

                    Assert.NotNull(value);
                    Assert.Equal(item.CounterValue, value);
                }
            }
        }
        
        [Fact]
        public void Can_use_get_counters()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
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
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

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

        [Fact]
        public void Should_error_if_attachment_doesnt_exist()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"

var doc = loadToUsers(this);
doc.addCounter(loadCounter('likes'));
"
                );
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

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

                AssertCounters(dest, new[]
                {
                    ("users/2", "likes", 1L, false)
                });

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.Load<User>("users/1"));
                    Assert.Null(session.Load<User>("users/3"));
                }
            }
        }

        [Fact]
        public void Can_use_has_counter()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
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
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

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

        [Fact]
        public void Must_not_send_counters_and_counter_tombstones_from_non_relevant_collections()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

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

        [Fact]
        public void Should_send_counter_even_if_doc_was_updater_later()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
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

                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));

                    Assert.NotNull(session.CountersFor("users/1").Get("likes"));
                }
            }
        }

        [Fact]
        public void Should_send_updated_counter_values()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 1);
                
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

        [Fact]
        public void Should_skip_counter_if_has_lower_etag_than_document()
        {
            using (var src = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "2"
            }))
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }
                
                etlDone.Wait(TimeSpan.FromMinutes(1));

                etlDone.Reset();

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

                etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

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

        [Theory]
        [InlineData("Users")]
        [InlineData(null)]
        public void Should_send_all_counters_on_doc_update(string collection = null)
        {
            using (var src = GetDocumentStore( new Options()
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

                if (collection == null)
                    AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);
                else
                    AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LastProcessedEtag >= 8);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));

                    long? value = session.CountersFor("users/1").Get("likes");

                    Assert.Equal(1, value);
                }
            }
        }
    }
}
