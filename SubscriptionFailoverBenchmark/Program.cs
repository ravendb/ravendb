using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using System;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure;
using FastTests.Server.Documents.Notifications;
using System.Collections.Generic;

namespace SubscriptionFailoverBenchmark
{
    public class SubscriptionFailoverTest: ClusterTestBase
    {
        public async Task RunTest()
        {
            var leader = await CreateRaftClusterAndGetLeader(5,false);

            var defaultDatabase = "StressTest";
            const int nodesAmount = 5;
            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrls[0]).ConfigureAwait(false);

            Task[] docsCreationTasks = new Task[3];
            Task[] subscriptionsTasks = new Task[5];
            List<(string, DateTime)>[] subscriptionSummaries = new List<(string, DateTime)>[5];

            var cts = new CancellationTokenSource();

            for(var i=0; i< docsCreationTasks.Length; i++)
            {
                var curI = i;
                docsCreationTasks[i] = Task.Run(async () =>
                {
                    await GenerateDocumentsForNode(defaultDatabase, docsCreationTasks, curI);
                });
            }

            var shutdownTask = Task.Run(async () =>
            {
                await ChaosMonkey(docsCreationTasks, cts);
            });

            for (var i=0; i< 5; i++)
            {
                var curI = i;
                var log = new List<(string, DateTime)>();
                subscriptionSummaries[i] = log;
                subscriptionsTasks[i] = Task.Run(async () =>
                {
                    await RunSubscription(defaultDatabase, curI, log);
                });
            }

            Task.WaitAll(subscriptionsTasks);

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

        private async Task RunSubscription(string defaultDatabase, int i, List<(string, DateTime)> log)
        {
            using (var store = new DocumentStore
            {
                Url = Servers[i].WebUrls[0],
                DefaultDatabase = defaultDatabase
            }.Initialize())
            {
                var subscriptionId = await store.AsyncSubscriptions.CreateAsync<User>(
                    new SubscriptionCreationParams<User>());

                var subscripiton = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));
                var counter = 0;
                var tcs = new TaskCompletionSource<bool>();

                log = new List<(string, DateTime)>();

                subscripiton.Subscribe(x =>
                {
                    Interlocked.Increment(ref counter);
                });

                subscripiton.AfterAcknowledgment += () =>
                {
                    if (counter == 1 * 1000 * 1000)
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

        private async Task GenerateDocumentsForNode(string defaultDatabase, Task[] docsCreationTasks, int i)
        {
            using (var store = new DocumentStore
            {
                Url = Servers[i].WebUrls[0],
                DefaultDatabase = defaultDatabase
            }.Initialize())
            {
                using (var bi = store.BulkInsert())
                {
                    for (var j = 0; j < 1 * 1000 * 1000 / docsCreationTasks.Length; j++)
                    {
                        var shouldRetry = false;
                        do
                        {
                            try
                            {
                                await bi.StoreAsync(new User
                                {
                                    Name = "John" + j
                                });
                                shouldRetry = false;
                            }
                            catch (Exception)
                            {
                                await Task.Delay(1000);
                                shouldRetry = true;
                            }
                        } while (shouldRetry);
                    }
                }
            }
        }

        private async Task ChaosMonkey(Task[] docsCreationTasks, CancellationTokenSource cts)
        {
            var curIndex = 0;
            while (cts.IsCancellationRequested == false)
            {
                var curServer = Servers[curIndex];
                var curConfig = curServer.Configuration;

                curServer.Dispose();

                await Task.Delay(10000);

                Servers[curIndex] = new Raven.Server.RavenServer(curConfig);

                curIndex = (curIndex + 1) % docsCreationTasks.Length;
            }
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            new SubscriptionFailoverTest().RunTest().Wait();
        }
    }
}