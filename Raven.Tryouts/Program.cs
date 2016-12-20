using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Changes;
using Raven.Client.Document;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        public static void Main(string[] args)
        {
            Console.ReadLine();
            using (var store = new DocumentStore
            {
                Url = "http://scratch1:8080",
                DefaultDatabase = "FooBar123"
            })
            {
                store.FailoverServers = new FailoverServers
                {
                    ForDatabases = new Dictionary<string, ReplicationDestination[]>
                    {
                        {
                            "FooBar123",
                            new[]
                            {
                                new ReplicationDestination
                                {
                                    Url = "http://scratch2:8080"
                                }
                            }
                        }
                    }
                };

                store.Conventions.FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries;
                store.Initialize();

                //for (int i = 0; i < 1000; i++)
                //{
                //    using (var session = store.OpenSession())
                //    {
                //        session.Store(new User
                //        {
                //            FirstName = "first-" + i,
                //            LastName = "last"
                //        });
                //        session.SaveChanges();
                //        Console.WriteLine(i);
                //    }
                //}

                for (int i = 0; i < 1000; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var doc = session.Query<User>().FirstOrDefault(x => x.FirstName == "first-" + i);
                        if (doc == null)
                            throw new ApplicationException("Missed doc with first name 'first-" + i + "'");
                        Console.WriteLine(i);
                    }
                }
            }
        }

        private static async Task DoTestAsync(DocumentStore store, int index)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    FirstName = "name-" + index,
                    LastName = "last"
                });
                await session.SaveChangesAsync();
            }
        }


        private static void DoTest(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                    FirstName = "name",
                    LastName = "last"
                });
                session.SaveChanges();
            }
        }


        static int id = 1;
        public static void BulkInsert(DocumentStore store, int numOfItems)
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < numOfItems; i++)
                    bulkInsert.Store(new User
                    {
                        FirstName = String.Format("First Name - {0}", i),
                        LastName = String.Format("Last Name - {0}", i)
                    }, String.Format("users/{0}", id++));
            }
        }
    }
}