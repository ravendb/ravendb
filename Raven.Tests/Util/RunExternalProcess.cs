using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Commands;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Util
{
    public class RunExternalProcess
    {
        [Fact]
        public void can_use_RavenDB_in_a_remote_process()
        {
            var documentConvention = new DocumentConvention();

            using (var driver = new RavenDBDriver("HelloShard", documentConvention))
            {
                driver.Start();

                using (var store = new DocumentStore()
                {
                    Url = driver.Url,
                    Conventions = documentConvention
                })
                {
                    store.Initialize();

                    using (var session = store.OpenSession())
                    {
                        session.Store(new Tuple<string, string>("hello", "world"));
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var result = session.Query<Tuple<string, string>>().Customize(q => q.WaitForNonStaleResultsAsOfNow()).Single();

                        Assert.Equal("hello", result.Item1);
                        Assert.Equal("world", result.Item2);
                    }
                }
            }
        }


        [Fact]
        public void can_use_RavenDB_in_a_remote_process_for_batch_operations()
        {
            var documentConvention = new DocumentConvention();

            using (var driver = new RavenDBDriver("HelloShard", documentConvention))
            {
                driver.Start();

                using (var store = new DocumentStore()
                {
                    Url = driver.Url,
                    Conventions = documentConvention
                })
                {
                    store.Initialize();

                    using (var session = store.OpenSession())
                    {
                        session.Advanced.DatabaseCommands.Batch(new [] {new PutCommandData()
                            {
                                Document = RavenJObject.FromObject(new Tuple<string, string>("hello", "world")),
                                Metadata = new RavenJObject(new KeyValuePair<string, RavenJToken>[]
                                {
                                    new KeyValuePair<string, RavenJToken>("Raven-Entity-Name", new RavenJValue("TuplesOfStringsOfStrings")), 
                                    new KeyValuePair<string, RavenJToken>("Raven-Clr-Type", new RavenJValue("System.Tuple`2[[System.String, mscorlib][System.String, mscorlib]], mscorlib")), 
                                })
                            }
                        });
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var result = session.Query<Tuple<string, string>>().Customize(q => q.WaitForNonStaleResultsAsOfNow()).Single();

                        Assert.Equal("hello", result.Item1);
                        Assert.Equal("world", result.Item2);
                    }
                }
            }
        }
    }
}