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
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Bugs;
using Raven.Tests.Common;
using Raven.Tests.Core.Replication;
using Raven.Tests.Helpers;
using Raven.Tests.Issues;
using Xunit;

namespace Tryouts
{

    public class ReplicationTest : ReplicationBase
    {
        private const int TestDuration = 600000;
        
        public void CanChangeClientRejectModeOnFailoverNode()
        {
            pathsToDelete.Add("~/Databases");
            
            using (var source = CreateStore(runInMemory:false))
            using (var destination = CreateStore(runInMemory: false))
            {
                
                Console.WriteLine(source.Url);
                Console.WriteLine(destination.Url);
                
                source.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                    null, new RavenJObject
                    {
                        {
                            "Destinations", new RavenJArray(new []
                            {
                                new RavenJObject
                                {
                                    { "Url", destination.Url },
                                    { "Database", destination.DefaultDatabase }
                                }
                            }.ToList())
                        },
                        {
                            "ClientConfiguration", new RavenJObject
                            {
                                { "FailoverBehavior", "ReadFromAllServers"}
                            }
                        }
                    }, new RavenJObject());

                source.GetReplicationInformerForDatabase().RefreshReplicationInformation((ServerClient)source.DatabaseCommands);
                destination.GetReplicationInformerForDatabase().RefreshReplicationInformation((ServerClient)destination.DatabaseCommands);
                //destination.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                //    null, new RavenJObject
                //    {
                //        {
                //            "ClientConfiguration", new RavenJObject
                //            {
                //                { "FailoverBehavior", "ReadFromAllServers"}
                //            }
                //        }
                //    }, new RavenJObject());


                var shouldSend = true;

                using (var session = source.OpenSession())
                {
                    try
                    {
                        session.Store(new User()
                        {
                            Name = "Vasia"
                        });
                        session.SaveChanges();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error");
                    }
                }


                var count = 0;
                var requestsTask = Task.Run(async () =>
                {
                    var sp = Stopwatch.StartNew();

                    while (sp.ElapsedMilliseconds < TestDuration)
                    {
                        if (shouldSend == false)
                        {
                            await Task.Delay(500);
                            continue;
                        }
                        using (var session = source.OpenSession())
                        {
                            try
                            {
                                count = session.Query<User>().Count();
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("Error");
                            }
                        }
                        await Task.Delay(500);

                        Console.WriteLine("Success");
                    }
                });


                var shouldToggle = false;
                

                var rejectClientModeTask = Task.Run(async () =>
                {
                    var stores = new[] { source, destination };
                    var toggleCounter = 0;
                    var sp = Stopwatch.StartNew();

                    while (sp.ElapsedMilliseconds < TestDuration)
                    {
                        if (shouldToggle == false)
                        {
                            await Task.Delay(500);
                            continue;
                        }
                        toggleCounter++;
                        var curStore = stores[toggleCounter % 2];
                        Console.WriteLine("Rejecting in: " + curStore.Url );
                        using (var request = curStore.DatabaseCommands.ForSystemDatabase().CreateRequest("/admin/databases-toggle-reject-clients?id=" + source.DefaultDatabase + "&isRejectClientsEnabled=true", "POST"))
                        {
                            try
                            {
                                request.ExecuteRequest();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex);
                            }
                        }
                            await Task.Delay(10000);
                        using (var request = curStore.DatabaseCommands.ForSystemDatabase().CreateRequest("/admin/databases-toggle-reject-clients?id=" + source.DefaultDatabase + "&isRejectClientsEnabled=false", "POST"))
                        {
                            try
                            {
                                request.ExecuteRequest();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Error toggling");
                            }
                        }
                    }
                });

                var shouldToggleTask = Task.Run(() =>
                {
                    ConsoleKeyInfo key;
                    do
                    {
                        key = Console.ReadKey();
                        if (key.Key == ConsoleKey.T)
                            shouldToggle = !shouldToggle;
                        if (key.Key == ConsoleKey.S)
                            shouldSend = !shouldSend;
                    } while (key.Key != ConsoleKey.Escape);
                });
                
                Task.WaitAll(new[] {requestsTask, rejectClientModeTask, shouldToggleTask});
            }
        }
    }
    public class Program
    {
        public class User
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        public static void Main(string[] args)
        {
           new ReplicationTest().CanChangeClientRejectModeOnFailoverNode();
        }

        private static void TestASyncSafer()
        {
            using (var masterStore = new DocumentStore()
            {
                ConnectionStringName = "RavenDB"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();


                while (sp.ElapsedMilliseconds < 10 * 60 * 1000)
                {
                    Console.WriteLine(sp.Elapsed);

                    try
                    {
                        using (var session = masterStore.OpenAsyncSession())
                        {
                            var loadAsyncTask = session.LoadAsync<RavenJObject>("products/1");
                            Task.WaitAll(loadAsyncTask);
                            var res = loadAsyncTask.Result;
                        }
                    }
                    catch (Exception ex)
                    {
                        Debugger.Launch();
                    }

                    Thread.Sleep(1000);
                }
            }
        }

        private static void TestASync()
        {
            using (var masterStore = new DocumentStore()
            {
                //ConnectionStringName = "RavenDB"
                Url="http://localhost:8080",
                DefaultDatabase = "Rep1"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();


                while (true)
                {
                    Console.Write(sp.Elapsed);

                    try
                    {
                        using (var session = masterStore.OpenAsyncSession())
                        {
                            var res = session.LoadAsync<RavenJObject>("docs/1").Result;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.Write(" Error");
                    }
                    Console.WriteLine();

                    Thread.Sleep(1000);
                }
            }
        }

        private static void TestSync()
        {
            using (var masterStore = new DocumentStore()
            {
                ConnectionStringName = "RavenDB"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();


                while (sp.ElapsedMilliseconds < 10*60*1000)
                {
                    Console.WriteLine(sp.Elapsed);

                    try
                    {
                        using (var session = masterStore.OpenSession())
                        {
                            var res = session.Load<dynamic>("docs/1");
                        }
                    }
                    catch (Exception ex)
                    {
                        Debugger.Launch();
                    }

                    Thread.Sleep(1000);
                }
            }
        }
    }
}