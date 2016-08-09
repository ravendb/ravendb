// -----------------------------------------------------------------------
//  <copyright file="TrafficRec.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using TrafficRecorder;

namespace Raven.Traffic
{
    public class TrafficRec
    {
        private readonly IDocumentStore store;
        private readonly TrafficToolConfiguration config;

        public TrafficRec(IDocumentStore store, TrafficToolConfiguration config)
        {
            this.store = store;
            this.config = config;
        }

        public void ExecuteTrafficCommand()
        {
            switch (config.Mode)
            {
                case TrafficToolConfiguration.TrafficToolMode.Record:
                    RecordRequests(config, store);
                    break;
                case TrafficToolConfiguration.TrafficToolMode.Replay:
                    ReplayRequests(config, store);
                    break;
            }
        }

        private void ReplayRequests(TrafficToolConfiguration config, IDocumentStore store)
        {
            Stream finalStream;
            var requestsCounter = 0;
            var skippedRequestsCounter = 0;
            var totalCountOfLogicRequests = 0;
            var totalSp = Stopwatch.StartNew();
            using (var stream = File.Open(config.RecordFilePath, FileMode.Open))
            {
                if (config.IsCompressed)
                {
                    finalStream = new GZipStream(stream, CompressionMode.Decompress, leaveOpen: true);
                }
                else
                {
                    finalStream = stream;
                }
                var trafficLogs =
                    JsonSerializer.Create().Deserialize<LogHttpRequestStatsParams[]>(new JsonTextReader(new StreamReader(finalStream)));

                ConcurrentQueue<string> queue = null;
                var cts = new CancellationTokenSource();
                var ct = cts.Token;
                Task outputTask = null;
                if (config.PrintOutput)
                {

                    queue = new ConcurrentQueue<string>();
                    outputTask = Task.Run(() =>
                    {
                        while (!ct.IsCancellationRequested || queue.Count != 0)
                        {
                            string message;
                            if (queue.TryDequeue(out message))
                            {
                                Console.WriteLine(message);
                            }
                            else
                            {
                                Thread.Sleep(10);
                            }
                        }
                    });
                }

                

                const string postLineSeparatorRegex = "\\t\\d: databases\\/[\\w\\.]+";
                const string endOfPostLineString = "\t\t\tQuery:";
                const string uriCleanRegex = "http://[\\w\\.-]+(:\\d*)?(\\/databases\\/[\\w\\.]+)?";
                
                Parallel.ForEach(trafficLogs, new ParallelOptions
                {
                    MaxDegreeOfParallelism = 60
                }, trafficLog =>
                {
                    var sp = Stopwatch.StartNew();
                    GetRequest[] requestsArray = null;

                    string uriString = Regex.Replace(trafficLog.RequestUri, uriCleanRegex, string.Empty);


                    string trafficQueryPart;
                    var trafficUrlPart = ExtractUrlAndQuery(uriString, out trafficQueryPart);

                    var curCount = Interlocked.Increment(ref requestsCounter);
                    if (ValidateUrlString(trafficUrlPart))
                    {
                        Interlocked.Increment(ref skippedRequestsCounter);
                        if (queue != null)
                        {
                            queue.Enqueue(string.Format("{0} out of {1}, skipped whole message",
                                curCount, trafficLogs.Length));
                        }
                        return;
                    }
                    Interlocked.Increment(ref totalCountOfLogicRequests);
                    if (trafficLog.HttpMethod.Equals("get", StringComparison.CurrentCultureIgnoreCase))
                    {
                        requestsArray = new[]
                        {
                            new GetRequest
                            {
                                Url = trafficUrlPart,
                                Query = trafficQueryPart
                            }
                        };
                    }
                    else if (trafficLog.CustomInfo != null)
                    {
                        var subArray = Regex.Split(trafficLog.CustomInfo.Replace("\r", string.Empty), postLineSeparatorRegex).Where(x => !String.IsNullOrEmpty(x)).Select(x =>
                        {
                            var endOfPostLastIndex = x.IndexOf(endOfPostLineString);
                            if (endOfPostLastIndex < 0)
                                return x;
                            return x.Remove(endOfPostLastIndex);
                        }).ToArray();
                        requestsArray =
                            subArray.Select(customInfoLine =>
                            {

                                trafficUrlPart = ExtractUrlAndQuery(customInfoLine, out trafficQueryPart);

                                if (ValidateUrlString(trafficUrlPart))
                                {
                                    if (queue != null)
                                    {
                                        queue.Enqueue(string.Format("{0} out of {1}, skipped inner message",
                                            curCount, trafficLogs.Length));
                                    }
                                    return null;
                                }

                                return new GetRequest
                                {
                                    Url = trafficUrlPart,
                                    Query = trafficQueryPart,
                                };
                            }).Where(x => x != null).ToArray();
                    }
                    Interlocked.Add(ref totalCountOfLogicRequests, requestsArray?.Length??0);
                    if (requestsArray == null || requestsArray.Length == 0)
                    {
                        Interlocked.Increment(ref skippedRequestsCounter);
                        if (queue != null)
                        {
                            queue.Enqueue(string.Format("{0} out of {1}, skipped",
                                curCount, trafficLogs.Length, sp.ElapsedMilliseconds, totalSp.ElapsedMilliseconds));
                        }
                        return;
                    }
                    try
                    {
                        
                        store.DatabaseCommands.MultiGet(requestsArray);
                        
                        
                        if (queue != null)
                        {
                            queue.Enqueue(string.Format("{0} out of {1}, took {2} ms. Total Time: {3} ms",
                                curCount, trafficLogs.Length, sp.ElapsedMilliseconds, totalSp.ElapsedMilliseconds));
                        }
                    }
                    catch (Exception e)
                    {
                        Interlocked.Increment(ref skippedRequestsCounter);
                        if (queue != null)
                        {
                            queue.Enqueue(string.Format("{0} out of {1}, failed, took {2} ms. Total Time: {3} ms",
                                curCount, trafficLogs.Length, sp.ElapsedMilliseconds, totalSp.ElapsedMilliseconds));
                        }
                    }
                });

                if (outputTask != null)
                {
                    cts.Cancel();
                    outputTask.Wait();
                }
            }

            Console.WriteLine(@"Summary: 
Requests sent: {0}
Requests skipped: {1}
Nested and non nested request: {2}
Total Time: {3}", requestsCounter, skippedRequestsCounter, totalCountOfLogicRequests, totalSp.ElapsedMilliseconds);

        }

        private static bool ValidateUrlString(string trafficUrlPart)
        {
            return (trafficUrlPart.StartsWith("/") || trafficUrlPart.StartsWith("\\")) && ValidateUrlFirstPathSegment(trafficUrlPart.Substring(1, trafficUrlPart.Length - 1))
                || ValidateUrlFirstPathSegment(trafficUrlPart);
        }

        private string ExtractUrlAndQuery(string uriString, out string trafficQueryPart)
        {
            string trafficUrlPart;
            var queryStartIndex = uriString.IndexOf("?");

            if (queryStartIndex <= 0)
            {
                trafficUrlPart = uriString;
                trafficQueryPart = uriString;
            }
            else
            {
                trafficUrlPart = uriString.Substring(0, queryStartIndex);
                trafficQueryPart = uriString.Substring(queryStartIndex);
            }
            return trafficUrlPart;
        }

        private static bool ValidateUrlFirstPathSegment(string trafficUrlPart)
        {
            return trafficUrlPart.StartsWith("bulk_docs") ||
                   trafficUrlPart.StartsWith("static") ||
                   trafficUrlPart.StartsWith("bulkInsert") ||
                   trafficUrlPart.StartsWith("changes") ||
                   trafficUrlPart.StartsWith("traffic-watch");
        }


        /// <summary>
        /// Connects to raven traffic event source and registers all the requests to the file defined in the config
        /// </summary>
        /// <param name="config">configuration conatining the connection, the file to write to, etc.</param>
        /// <param name="store">the store to work with</param>
        private void RecordRequests(TrafficToolConfiguration config, IDocumentStore store)
        {
            var requestsQueue = new ConcurrentQueue<RavenJObject>();
            var messagesCount = 0;
            var mre = new ManualResetEvent(false);
            var trafficWatchObserver = new TrafficWatchObserver(store,config.ResourceName, mre, config.Timeout, x =>
            {
                if (config.AmountConstraint.HasValue && Interlocked.Increment(ref messagesCount) > config.AmountConstraint.Value)
                    mre.Set();
                else
                    requestsQueue.Enqueue(x);
            });

            trafficWatchObserver.EstablishConnection();

            var writingTask = Task.Run(() =>
            {
                WriteRequestsFromQueueToFile(requestsQueue, config.RecordFilePath, config.IsCompressed,config.PrintOutput, mre);
            });



            if (config.DurationConstraint.HasValue)
                mre.WaitOne(config.DurationConstraint.Value);
            else
                mre.WaitOne();

            mre.Set();

            writingTask.Wait();
            trafficWatchObserver.OnCompleted();
        }

        private void WriteRequestsFromQueueToFile(ConcurrentQueue<RavenJObject> messages, string filePath, bool isCompressed, bool printOutput, ManualResetEvent mre)
        {
            RavenJObject notification;
            var requestsCounter = 0;
            using (var stream = File.Create(filePath))
            {
                Stream finalStream = stream;
                if (isCompressed)
                    finalStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true);

                using (var streamWriter = new StreamWriter(finalStream))
                {
                    var jsonWriter = new JsonTextWriter(streamWriter)
                    {
                        Formatting = Formatting.Indented
                    };
                    jsonWriter.WriteStartArray();
                    
                    while (messages.TryDequeue(out notification) || mre.WaitOne(0) == false)
                    {
                        if (notification == null)
                        {
                            Thread.Sleep(100);
                            continue;
                        }
                        requestsCounter++;
                        if (printOutput)
                        {
                            Console.WriteLine("Request #{0} Stored", requestsCounter);
                        }
                        notification.WriteTo(jsonWriter);
                    }
                    jsonWriter.WriteEndArray();
                    streamWriter.Flush();
                }

                if (isCompressed)
                    finalStream.Dispose();
            }
        }
    }
}
