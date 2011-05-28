using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database;
using Raven.Server;

namespace Raven.StackOverflow.Etl
{
    public class FileToRavenCommand : ICommand
    {
        public string CommandText
        {
            get { return "file"; }
        }

        public void Run()
        {
            LoadIntoRaven();
        }

        public void LoadArgs(IEnumerable<string> remainingArgs)
        {
        }

        public void WriteHelp(TextWriter tw)
        {
            Console.WriteLine("Raven.StackOverflow.Etl.exe file");
        }

        public static void LoadIntoRaven()
        {
            const string dataDirectory = @"C:\Work\ravendb\ETL\Raven.StackOverflow.Etl\bin\Debug\Data";
            if (Directory.Exists(dataDirectory))
                Directory.Delete(dataDirectory, true);

            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(9090);
            using (var ravenDbServer = new RavenDbServer(new RavenConfiguration
            {
                DataDirectory = dataDirectory,
                Port = 9090,
                AnonymousUserAccessMode = AnonymousUserAccessMode.All
            }))
            {
                ExecuteAndWaitAll(
                    LoadDataFor("Users*.json")
                    //,LoadDataFor("Posts*.json")
                    );
                ExecuteAndWaitAll(
                    LoadDataFor("Badges*.json")
                    //,LoadDataFor("Votes*.json"),
                    //LoadDataFor("Comments*.json")
                    );

                var indexing = Stopwatch.StartNew();
                Console.WriteLine("Waiting for indexing");
                while (ravenDbServer.Database.HasTasks)
                {
                    Console.Write(".");
                    Thread.Sleep(50);
                }
                Console.WriteLine();
                Console.WriteLine("Finishing indexing took: {0}", indexing.Elapsed);
            }


            foreach (var duration in Program.durations.GroupBy(x => x.Item1))
            {
                Console.WriteLine("{0} {1}", duration.Key, duration.Average(x => x.Item2.TotalMilliseconds));
            }
        }

        public static IEnumerable<Action> LoadDataFor(string searchPattern)
        {
            Console.WriteLine("Loading for {0}", searchPattern);
            var timeSpans = new List<TimeSpan>();
            foreach (var fileModifable in Directory.GetFiles(@"C:\Work\ravendb\ETL\Raven.StackOverflow.Etl\bin\Debug\Docs", searchPattern))
            {
                var file = fileModifable;
                yield return () =>
                {
                    var sp = Stopwatch.StartNew();
                    HttpWebResponse webResponse;
                    while (true)
                    {
                        var httpWebRequest = (HttpWebRequest)WebRequest.Create("http://localhost:9090/bulk_docs");
                        httpWebRequest.Method = "POST";
                        using (var requestStream = httpWebRequest.GetRequestStream())
                        {
                            var readAllBytes = File.ReadAllBytes(file);
                            requestStream.Write(readAllBytes, 0, readAllBytes.Length);
                        }
                        try
                        {
                            webResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                            webResponse.Close();
                            break;
                        }
                        catch (WebException e)
                        {
                            webResponse = e.Response as HttpWebResponse;
                            if (webResponse != null &&
                                webResponse.StatusCode == HttpStatusCode.Conflict)
                            {
                                Console.WriteLine("{0} - {1} - {2} - {3}", Path.GetFileName(file), sp.Elapsed, webResponse.StatusCode,
                                    Thread.CurrentThread.ManagedThreadId);
                                continue;
                            }

                            Console.WriteLine(new StreamReader(e.Response.GetResponseStream()).ReadToEnd());
                            throw;
                        }
                    }
                    var timeSpan = sp.Elapsed;
                    timeSpans.Add(timeSpan);
                    Program.durations.Add(new Tuple<string, TimeSpan>(searchPattern, timeSpan));
                    Console.WriteLine("{0} - {1} - {2} - {3}", Path.GetFileName(file), timeSpan, webResponse.StatusCode,
                        Thread.CurrentThread.ManagedThreadId);
                };
            }
        }

        public static void ExecuteAndWaitAll(params IEnumerable<Action>[] taskGenerators)
        {
            Parallel.ForEach(from generator in taskGenerators
                             from action in generator
                             select action,
                new ParallelOptions { MaxDegreeOfParallelism = 10 },
                action =>
                {
                    try
                    {
                        action();
                    }
                    catch (WebException e)
                    {
                        var readToEnd = new StreamReader(e.Response.GetResponseStream()).ReadToEnd();
                        Console.WriteLine(readToEnd);
                        throw;
                    }
                });

            //foreach (var act in from generator in taskGenerators
            //                              from action in generator
            //                              select action)
            //{
            //    act();
            //}
        }
    }
}
