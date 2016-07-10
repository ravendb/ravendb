using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace SubscriptionsBenchmark
{
    public class ParallelSubscriptionsBenchmark
    {
        public async Task Run(string url="http://localhost:8080", string defaultDatabase="freeDB", int maxItems=100000, string collectionName="Disks",
            int parallelism = 10)
        {
            var cq = new ConcurrentQueue<long>();
            using (var store = new DocumentStore
            {
                Url = url,
                DefaultDatabase = defaultDatabase
            }.Initialize())
            {
                var cd = new CountdownEvent(parallelism);


                var tasks = Enumerable.Range(1, parallelism).Select(j =>
                    Task.Run(async () =>
                    {
                        Console.WriteLine($"Entered Task {j}");
                        if (cd.CurrentCount != 0)
                        {
                            cd.Signal();
                            cd.Wait();
                        }
                        Console.WriteLine("Started");
                        var subscriptionId = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCriteria
                        {
                            Collection = collectionName
                        });

                        var sp = Stopwatch.StartNew();
                        for (var i = 0; i < 3; i++)
                        {
                            var counterObserver = new CounterObserver(maxItems);

                            using (var subscription = store.AsyncSubscriptions.Open(
                                new SubscriptionConnectionOptions
                                {
                                    SubscriptionId = subscriptionId,
                                }))
                            {
                                subscription.Subscribe(counterObserver);
                                await subscription.StartAsync();

                                await counterObserver.Tcs.Task;
                                await subscription.DisposeAsync();
                                cq.Enqueue(sp.ElapsedMilliseconds);
                                sp.Restart();
                            }
                        }
                    })).ToArray();

                Task.WaitAll(tasks, CancellationToken.None);

                foreach (var curTime in cq)
                    Console.WriteLine(curTime);
            }
        }
    }
}
