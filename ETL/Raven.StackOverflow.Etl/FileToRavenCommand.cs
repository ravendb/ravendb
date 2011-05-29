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
using Raven.Database.Config;
using Raven.Http;
using Raven.Server;

namespace Raven.StackOverflow.Etl
{
    public class FileToRavenCommand : ICommand
    {
        public string CommandText
        {
            get { return "file2raven"; }
        }

        public string InputDirectory { get; private set; }
        public string OutputRavenUrl { get; private set; }

        public void Run()
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
        }

        public IEnumerable<Action> LoadDataFor(string searchPattern)
        {
            Console.WriteLine("Loading {0}.", Path.Combine(InputDirectory, searchPattern ?? "*"));
            var timeSpans = new List<TimeSpan>();
            foreach (var fileModifable in Directory.GetFiles(InputDirectory, searchPattern))
            {
                var file = fileModifable;
                yield return () =>
                {
                    var sp = Stopwatch.StartNew();
                    HttpWebResponse webResponse;
                    while (true)
                    {

                        var httpWebRequest = (HttpWebRequest)WebRequest.Create(new Uri(new Uri(OutputRavenUrl), "bulk_docs"));
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

        public void LoadArgs(string[] remainingArgs)
        {
            if (remainingArgs.Count() != 2)
                throw new Exception("");

            InputDirectory = remainingArgs[0];

            if (!Directory.Exists(InputDirectory))
                throw new ArgumentException("Input directory not found.");

            OutputRavenUrl = remainingArgs[1];

            if (!new Uri(OutputRavenUrl).Scheme.Equals("http", StringComparison.InvariantCultureIgnoreCase))
                throw new ArgumentException("RavenDB url expected as second parameter");
        }

		public void WriteHelp(TextWriter tw)
        {
            Console.WriteLine("Raven.StackOverflow.Etl.exe " + CommandText + " <inputDirectory> <outputRavenUrl>");
        }
    }
}
