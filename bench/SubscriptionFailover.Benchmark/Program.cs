using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using System;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure;
using FastTests.Server.Documents.Notifications;
using System.Collections.Generic;
using Voron.Util;

namespace SubscriptionFailoverBenchmark
{
    public class SubscriptionFailoverBenchmark: ClusterTestBase
    {

        public async Task RunTestSimple()
        {
            using (var store = GetDocumentStore())
            {

                var subscriptionTask = RunsSubscriptionSimple(store);
                var documentsTask = GenerateDocuments(store);
                await Task.WhenAll(subscriptionTask, documentsTask);
            }
        }

        private async Task GenerateDocuments(DocumentStore store)
        {
            var j = 0;
            
            do
            {
                try
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        for (var i=0 ; i < 1000 ; j++,i++)
                        {
                            await session.StoreAsync(new User
                            {
                                Name = "User"+j
                            });
                        }

                        await session.SaveChangesAsync();
                    }
                }
                catch (Exception)
                {
                    await Task.Delay(1000);

                }
            } while (j < DocsAmount);
        
        }

        public async Task RunsSubscriptionSimple(DocumentStore store)
        {
            var subscriptionId = await store.AsyncSubscriptions.CreateAsync<User>(
                new SubscriptionCreationOptions<User>());

            var subscripiton = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
            {
                MaxDocsPerBatch = 1024
            });
            var counter = 0;
            var tcs = new TaskCompletionSource<bool>();

            

            subscripiton.Subscribe(x =>
            {
                Interlocked.Increment(ref counter);
            });

            subscripiton.AfterAcknowledgment += () =>
            {
                Console.WriteLine($"{ subscriptionId}: {counter}");
                if (counter == DocsAmount)
                    tcs.SetResult(true);
            };

            await subscripiton.StartAsync();

            await tcs.Task;
        }

        const int DocsAmount = 1000000;
        const int nodesAmount = 3;
        const int docsCreationTasksAmount = 3;
        const int subscriptionCreationTasksAmount = 5;
        public async Task RunTest()
        {
            var defaultDatabase = "StressTest";

            
            
            var leader = await CreateRaftClusterAndGetLeader(nodesAmount, false);

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrls[0]).ConfigureAwait(false);

            var docsCreationTasks = new Task[docsCreationTasksAmount];
            
            var subscriptionsTasks = new Task[subscriptionCreationTasksAmount];
            var subscriptionSummaries = new List<(string, DateTime)>[subscriptionCreationTasksAmount];

            var cts = new CancellationTokenSource();

            for(var i=0; i< docsCreationTasks.Length; i++)
            {
                var curI = i;
                docsCreationTasks[i] = Task.Run(async () =>
                {
                    await GenerateDocumentsForNode(defaultDatabase, docsCreationTasks, Servers[curI].WebUrls[0], curI * DocsAmount / nodesAmount);
                });
            }

            var shutdownTask = Task.Run((Func<Task>)(async () =>
            {
                await ChaosMonkey(cts);
            }));

            for (var i=0; i< subscriptionsTasks.Length; i++)
            {
                var curI = i;
                var log = new List<(string, DateTime)>();
                subscriptionSummaries[i] = log;
                subscriptionsTasks[i] = Task.Run(async () =>
                {
                    await RunSubscription(defaultDatabase, log);
                });
            }

            Task.WaitAll(subscriptionsTasks.Concat(shutdownTask));

            foreach (var summary in subscriptionSummaries)
            {
                Console.WriteLine("Start");

                foreach (var item in summary)
                {
                    Console.WriteLine($"{item.Item1}:{item.Item2}");
                }

                Console.WriteLine("End");
            }
        }

        private async Task RunSubscription(string defaultDatabase,  List<(string, DateTime)> log)
        {
            using (var store = new DocumentStore
            {
                Urls = Servers[0].WebUrls,
                Database = defaultDatabase
            }.Initialize())
            {
                var subscriptionId = await store.AsyncSubscriptions.CreateAsync<User>(
                    new SubscriptionCreationOptions<User>());

                var subscripiton = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                {
                    MaxDocsPerBatch = 1024
                });
                var counter = 0;
                var tcs = new TaskCompletionSource<bool>();

                log = new List<(string, DateTime)>();

                subscripiton.Subscribe(x =>
                {
                    Interlocked.Increment(ref counter);
                });

                subscripiton.AfterAcknowledgment += () =>
                {
                    Console.WriteLine($"{ subscriptionId}: {counter}");
                    if (counter == 1 * DocsAmount)
                        tcs.SetResult(true);
                };

                subscripiton.SubscriptionConnectionInterrupted += (Exception ex, bool willReconnect) =>
                {
                    log.Add(("Interrupted", DateTime.Now));
                };

                subscripiton.ConnectionEstablished += () =>
                {
                    log.Add(("Established", DateTime.Now));
                };

                await subscripiton.StartAsync();

                await tcs.Task;
            }            
        }

        private async Task GenerateDocumentsForNode(string defaultDatabase, Task[] docsCreationTasks, string url, long rangeStart)
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { url },
                Database = defaultDatabase
            }.Initialize())
            {
                var j = 0;
                do
                {
                    try
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            for (var k = 0; k < 1000; j++, k++)
                            {
                                await session.StoreAsync(new User
                                {
                                    Name = "User" + j
                                },$"Users/{rangeStart+j}");
                            }

                            await session.SaveChangesAsync();
                        }
                    }
                    catch (Exception)
                    {
                        await Task.Delay(1000);

                    }
                } while (j < DocsAmount / nodesAmount + 3);
            }
        }

        private async Task ChaosMonkey( CancellationTokenSource cts)
        {
            var curIndex = 0;
            //while (cts.IsCancellationRequested == false)
            //{
            //    var curServer = Servers[curIndex];
            //    var curConfig = curServer.Configuration;

            //    curServer.Dispose();

            //    await Task.Delay(30000);

            //    Servers[curIndex] = new Raven.Server.RavenServer(curConfig);

            //    curIndex = (curIndex + 1) % nodesAmount;
            //}
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            new SubscriptionFailoverBenchmark().RunTest().Wait();
        }
    }
}