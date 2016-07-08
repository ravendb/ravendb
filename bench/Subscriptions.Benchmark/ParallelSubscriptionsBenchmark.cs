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
    public class ActionObserver<T> : IObserver<T>
    {
        private readonly Action<T> _onNext;
        private readonly Action _onCompleted;
        private readonly Action<Exception> _onError;

        public ActionObserver(Action<T> onNext, Action onCompleted = null, Action<Exception> onError = null)
        {
            _onNext = onNext;
            _onCompleted = onCompleted;
            _onError = onError;
        }

        public void OnCompleted()
        {
            _onCompleted?.Invoke();
        }

        public void OnError(Exception error)
        {
            _onError?.Invoke(error);
        }

        public void OnNext(T value)
        {
            _onNext(value);
        }
    }

    public class Apple
    {
        public string Name { get; set; }
    }
    public class ParallelSubscriptionsBenchmark
    {
        public async Task Run(string url, string defaultDatabase, int maxItems, string collectionName,
            int parallelism)
        {
            using (var store = new DocumentStore
            {
                Url = url,
                DefaultDatabase = defaultDatabase
            }.Initialize())
            {
                var databaseNames = store.DatabaseCommands.GlobalAdmin.GetDatabaseNames(1024);

                if (databaseNames.Contains(defaultDatabase) == false)
                {
                    var dbDoc = MultiDatabase.CreateDatabaseDocument(defaultDatabase);
                    store.DatabaseCommands.GlobalAdmin.CreateDatabase(dbDoc);

                    using (var bi = store.BulkInsert())
                    {
                        for (var i = 0; i < maxItems; i++)
                        {
                            AsyncHelpers.RunSync(() => bi.StoreAsync(new Apple()));
                        }
                    }

                    Console.WriteLine("Database does not exist, cannot perform test");
                    return;
                }

                var collectionStatsStream =
                    AsyncHelpers.RunSync(async () =>
                    {
                        var collectionStatsResponse =
                            await
                                store.DatabaseCommands.CreateRequest("/collections/stats", HttpMethod.Get)
                                    .ExecuteRawResponseAsync();
                        return await collectionStatsResponse.Content.ReadAsStreamAsync();
                    });

                var collectionStatsObject =
                    RavenJObject.ReadFrom(
                        new RavenJsonTextReader(
                            new StreamReader(collectionStatsStream)));

                var collectionsAmounts = collectionStatsObject.Value<RavenJObject>("Collections");
                RavenJToken collectionSizeToken;
                if (collectionsAmounts.TryGetValue(collectionName, out collectionSizeToken) == false)
                {
                    Console.WriteLine("Collection not found, cannot perform test");
                    return;
                }

                Console.WriteLine(maxItems);
                maxItems = Math.Min(maxItems,
                    collectionSizeToken.Value<int>());
                Console.WriteLine(maxItems);
                if (maxItems == 0)
                {
                    Console.WriteLine("Collection has no items, cannot perform test");
                    return;
                }
            }

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


                        var sp = Stopwatch.StartNew();
                        for (var i = 0; i < 50; i++)
                        {
                            Console.WriteLine($"Started Task {j} iteration {i}");

                            TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();
                            int proccessedDocuments = 0;
                            var subscriptionId = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCriteria
                            {
                                Collection = collectionName
                            });


                                var actionObserver = new ActionObserver<RavenJObject>(onNext:
                                    x =>
                                    {
                                    // Console.WriteLine(x.ToString());                     
                                    if (Interlocked.Increment(ref proccessedDocuments) == maxItems)
                                        {
                                            if (tcs.Task.IsCompleted)
                                                return;

                                            lock (tcs)
                                            {
                                                if (tcs.Task.IsCompleted == false)
                                                    tcs.SetResult(true);
                                            }
                                        }
                                    },
                                    onCompleted: () =>
                                    {
                                        if (tcs.Task.IsCompleted)
                                            return;

                                        lock (tcs)
                                        {
                                            if (tcs.Task.IsCompleted == false)
                                                tcs.SetResult(true);
                                        }
                                    },
                                    onError: x => { Console.WriteLine(x); });

                            using (var subscription = store.AsyncSubscriptions.Open(
                                new SubscriptionConnectionOptions
                                {
                                    SubscriptionId = subscriptionId,
                                }))
                            {
                                subscription.Subscribe(actionObserver);
                                subscription.Start();

                                await tcs.Task;
                                await subscription.DisposeAsync();
                                cq.Enqueue(sp.ElapsedMilliseconds);
                                Console.WriteLine($"Done task {j} iteration {i}");
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
