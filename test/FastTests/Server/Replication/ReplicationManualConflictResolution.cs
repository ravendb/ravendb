using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class ManualConflictResolution : ReplicationTestsBase
    {
        [Fact]
        public void CanManuallyResolveConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetToManualResolution(master, slave, "return {Name:docs[0].Name + '123'};");
                SetupReplication(master, slave);

                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var updated = WaitForDocument(slave, "users/1");
                Assert.True(updated);

                using (var session = slave.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }
     
                var updated2 = WaitForDocument(slave, "users/1");
                Assert.True(updated2);

                using (var session = slave.OpenSession())
                {
                    try
                    {
                        var item = session.Load<ReplicationConflictsTests.User>("users/1");
                        Assert.Equal(item.Name, "Karmeli123");
                    }
                    catch (ErrorResponseException e)
                    {
                        Assert.Equal(HttpStatusCode.Conflict, e.StatusCode);
                        //var list = new List<JsonDocument>();
                        //for (int i = 0; i < e.ConflictedVersionIds.Length; i++)
                        //{
                        //	var doc = slave.DatabaseCommands.Get(e.ConflictedVersionIds[i]);
                        //	list.Add(doc);
                        //}

                        //var resolved = list[0];
                        ////TODO : when client API is finished, refactor this so the test works as designed
                        ////resolved.Metadata.Remove(Constants.RavenReplicationConflictDocument);
                        //slave.DatabaseCommands.Put("users/1", null, resolved.DataAsJson, resolved.Metadata);
                    }
                }
            }
        }


        [Fact]
        public void ManuallyResolveToTombstone()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetToManualResolution(master, slave,"return null;");
                SetupReplication(master, slave);

                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var updated = WaitForDocument(slave, "users/1");
                Assert.True(updated);

                using (var session = slave.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }

                var updated2 = WaitForDocumentDeletion(slave, "users/1");
                Assert.True(updated2);
            }
        }

        [Fact]
        public void ManuallyResolveCopmlex()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetToManualResolution(master, slave, @"

function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}

    var names = [];
    for(var i = 0; i < docs.length; i++) 
    {
        names = names.concat(docs[i].Name.split(' '));
    }
            return {
                Name: names.filter(onlyUnique).join(' '),
                Age: Math.max.apply(Math,docs.map(function(o){return o.Age;}))
            }

");
                SetupReplication(master, slave);
                long? etag;
                using (var session = slave.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel",
                        Age = 12
                    }, "users/1");
                    session.SaveChanges();
                    etag = session.Advanced.GetEtagFor(session.Load<ReplicationConflictsTests.User>("users/1"));
                }
            
                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel",
                        Age = 123
                    }, "users/1");
                    session.SaveChanges();
                }


                var update = WaitForBiggerEtag(slave, etag);
                Assert.True(update);

                using (var session = slave.OpenSession())
                {
                    try
                    {
                        var item = session.Load<ReplicationConflictsTests.User>("users/1");
                        Assert.Equal("Karmel",item.Name);
                        Assert.Equal(123, item.Age);
                    }
                    catch (ErrorResponseException e)
                    {
                        Assert.Equal(HttpStatusCode.Conflict, e.StatusCode);
                    }
                }
            }
        }

        [Fact]
        public void ManuallyResolveIgnore()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetToManualResolution(master, slave, @"

return;

");
                SetupReplication(master, slave);
                long? etag;
                using (var session = slave.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel",
                        Age = 12
                    }, "users/1");
                    session.SaveChanges();
                    etag = session.Advanced.GetEtagFor(session.Load<ReplicationConflictsTests.User>("users/1"));
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel",
                        Age = 123
                    }, "users/1");
                    session.SaveChanges();
                }

                var update = WaitForBiggerEtag(slave, etag);
                Assert.False(update);

            }
        }

        public bool WaitForBiggerEtag(DocumentStore store,long? etag)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < 10000)
            {
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<ReplicationConflictsTests.User>("users/1");
                    if (session.Advanced.GetEtagFor(doc) > etag)
                        return true;
                }
                Thread.Sleep(10);
            }
            return false;
        }

        public void SetToManualResolution(DocumentStore master, DocumentStore slave, string script)
        {
            using (var session = slave.OpenSession())
            {
                var destinations = new List<ReplicationDestination>();
                session.Store(new ReplicationDocument
                {
                    Destinations = destinations,
                    DocumentConflictResolution = StraightforwardConflictResolution.ResolveManually,

                }, Constants.Replication.DocumentReplicationConfiguration);
                session.Store(new ReplicationManualResolver
                {
                    ResolveByCollection = new Dictionary<string, ScriptResolver>{
                            { "Users", new ScriptResolver
                                {
                                    Script = script
                                }
                            }
                        }
                }, Constants.Replication.DocumentReplicationResolvers);
                session.SaveChanges();
            }
        }
    }
}
