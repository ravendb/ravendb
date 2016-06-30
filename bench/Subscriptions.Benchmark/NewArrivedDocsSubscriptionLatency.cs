using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace SubscriptionsBenchmark
{
    public class NewArrivedDocsSubscriptionLatency:IDisposable
    {
        private DocumentStore _store;
        private int? _batchSize;
        private int? _minDocsCount;
        private int? _maxDocsCount;
        private long _subsId;
        private int _docsCounter = 0;

        public NewArrivedDocsSubscriptionLatency(string[] args, string url="http://localhost:8080", string databaseName="vanillaDB")
        {

            if (args.Length > 0)
            {
                _batchSize = Int32.Parse(args[0]);
                if (args.Length > 2)
                {
                    _minDocsCount = Int32.Parse(args[1]);
                    _maxDocsCount = Int32.Parse(args[2]);
                }
            }
            _store = new DocumentStore()
            {
                Url = url,
                DefaultDatabase = databaseName
            };
            _store.Initialize();

            if (_store.DatabaseCommands.GlobalAdmin.GetDatabaseNames(1024).Contains(databaseName))
            {
                _store.DatabaseCommands.GlobalAdmin.DeleteDatabase(databaseName,true);
            }
            _store.DatabaseCommands.GlobalAdmin.CreateDatabase(MultiDatabase.CreateDatabaseDocument(databaseName));

            _subsId = _store.Subscriptions.Create(new SubscriptionCriteria()
            {
                Collection = "Cats"
            });
        }

        public void PerformBenchmark()
        {
            for (var i = _minDocsCount ?? 1; i < (_maxDocsCount?? 100); i++)
            {
                for (var j = 0; j < 3; j++)
                {
                    var result = AsyncHelpers.RunSync(()=>SingleTest(i));
                 
                    Console.WriteLine($"{i}:  {result}");
                }
            }
        }

        public async Task<RunResult> SingleTest(int docsCount)
        {
            var subscription = await _store.AsyncSubscriptions.OpenAsync(_subsId, new SubscriptionConnectionOptions()
            {
                MaxDocsPerBatch = _batchSize ?? docsCount
            });
            var observer = new CounterObserver(docsCount);
            subscription.Subscribe(observer);


            
            var commands = Enumerable.Range(0,docsCount).Select(x=> {
                var curId = $"Cats/{_docsCounter++}";

                var ravenJObject = new RavenJObject()
                {
                    {"Name",$"Cat_{_docsCounter}" },
                    {Constants.Metadata, new RavenJObject()
                        {
                            { Constants.Headers.RavenEntityName,"Cats"}
                        }
                    }
                };

                return new PutCommandData()
                {
                    Document = ravenJObject,
                    Key = curId
                };
            }).ToArray();

            var sp = Stopwatch.StartNew();
            await _store.AsyncDatabaseCommands.BatchAsync(commands.ToArray());

            await observer.Tcs.Task;

            var singleTest = new RunResult
            {
                DocsProccessed = observer.CurCount,
                DocsRequested = docsCount,
                ElapsedMs = sp.ElapsedMilliseconds
            };
            //await subscription.CloseSubscriptionAsync();
            Console.WriteLine("Before Dispose");
            await subscription.DisposeAsync();
            return singleTest;
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}
