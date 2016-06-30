using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace SubscriptionsBenchmark
{

    public class CounterObserver:IObserver<RavenJObject>
    {
        public int MaxCount { get; private set; }
        public int CurCount { get; private set; }

        public readonly TaskCompletionSource<bool> Tcs = new TaskCompletionSource<bool>();

        public CounterObserver(int maxCount)
        {
            MaxCount = maxCount;
            CurCount = 0;
        }
        public void OnCompleted()
        {
            if (Tcs.Task.IsCompleted)
                return;
            if (CurCount != MaxCount)
            {
                Tcs.SetResult(false);
            }
            else
            {
                Tcs.SetResult(true);
            }
        }

        public void OnError(Exception error)
        {
            Console.WriteLine(error);
        }

        public void OnNext(RavenJObject value)
        {
            if (Tcs.Task.IsCompleted)
                return;
            CurCount++;

            if (CurCount == MaxCount)
                Tcs.SetResult(true);

        }
    }

    public class RunResult
    {
        public int DocsRequested { get; set; }
        public long ElapsedMs { get; set; }
        public long DocsProccessed { get; set; }
        public override string ToString()
        {
            return $"Elapsed: {ElapsedMs}; MaxDocs: {DocsRequested}; ProccessedDocs: {DocsProccessed}";
        }
    }
    public class SingleSubscriptionBenchmark:IDisposable
    {
        private int? _batchSize;
        private int? _minPowOf10;
        private int? _maxPowOf10;
        private DocumentStore _store;

        public SingleSubscriptionBenchmark(string[] args, string url="http://localhost:8080", string databaseName="freeDB")
        {
            if (args.Length > 0)
            {
                _batchSize = Int32.Parse(args[0]);
                if (args.Length > 2)
                {
                    _minPowOf10 = Int32.Parse(args[1]);
                    _maxPowOf10 = Int32.Parse(args[2]);
                }
            }
            _store = new DocumentStore()
            {
                DefaultDatabase = databaseName,
                Url = url
            };
            _store.Initialize();

        }

        public void PerformBenchmark()
        {
            for (var i = _minPowOf10 ?? 1; i < (_maxPowOf10 ?? 6); i++)
            {
                for (var j=0; j<3; j++)
                    {
                        var executionTask = SingleTestRun((int)Math.Pow(10, i), _batchSize);
                        executionTask.Wait();
                        Console.WriteLine($"{(int)Math.Pow(10, i)}:  {executionTask.Result}");
                    }
            }
        }


        private async Task<RunResult> SingleTestRun(int docCount = 10000, int? batchSize = null)
        {
            
            var subscriptionId = await _store.AsyncSubscriptions.CreateAsync(new SubscriptionCriteria()
            {
                Collection = "Disks"
            });
            var subscription =
                await _store.AsyncSubscriptions.OpenAsync(subscriptionId, new SubscriptionConnectionOptions()
                {
                    MaxDocsPerBatch = batchSize??docCount
                });
            var observer = new CounterObserver(docCount);
            var sp = Stopwatch.StartNew();
            subscription.Subscribe(observer);
            await observer.Tcs.Task;
            
            await subscription.DisposeAsync();
            return new RunResult
            {
                DocsProccessed = observer.CurCount,
                DocsRequested = docCount,
                ElapsedMs = sp.ElapsedMilliseconds
            };

        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}
