using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Storage.Voron.Impl;

namespace ConsoleApplication4
{
    class Program
    {
        static void Main(string[] args)
        {
            var ds = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "Northwind"
            }.Initialize();
            var counters = 0;
            var tasks = new List<Task>();

            for (int i = 0; i < 1; i++)
            {
                var copy = i;
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    for (int j = 0; j < 10000; j++)
                    {
                        using (var s = ds.OpenSession())
                        {
                            s.Load<Company>(copy);
                            s.Query<Company>().Where(x => x.ExternalId == "ALFKI").ToList();
                        }

                        using (var s = ds.OpenSession())
                        {
                            var c = s.Load<Company>(copy);
                            if (j % 2 == 0)
                            {
                                c.ExternalId = c.ExternalId.ToLower();
                            }
                            else
                            {
                                c.ExternalId = c.ExternalId.ToUpper();
                            }
                            s.SaveChanges();
                        }

                        Interlocked.Increment(ref counters);
                    }
                }));

                tasks.Add(Task.Factory.StartNew(() =>
                {
                    for (int j = 0; j < 10000; j++)
                    {
                        using (ds.AggressivelyCache())
                        using (var s = ds.OpenSession())
                        {
                            s.Load<Company>(1);
                            s.Query<Company>().Where(x => x.ExternalId == "ALFKI").ToList();
                        }
                        Interlocked.Increment(ref counters);
                    }
                }));
            }
            var sp = Stopwatch.StartNew();
            while (Task.WaitAll(tasks.ToArray(), 1000) == false)
            {
                if (tasks.Any(x => x.IsFaulted))
                {
                    var aggregateExceptions = tasks.Where(x => x.IsFaulted).Select(x => x.Exception).ToList();
                    foreach (var aggregateException in aggregateExceptions)
                    {
                        Console.WriteLine(aggregateException);
                    }
                }

                int workerThreads;
                int completionPortThreads;
                ThreadPool.GetAvailableThreads(out workerThreads, out completionPortThreads);
                int maxWorkerThreads;
                int maxCompletionThreads;
                ThreadPool.GetMaxThreads(out maxWorkerThreads, out maxCompletionThreads);
                Console.Write("\r{0:#,#;;0} requests in {1:#,#.#;;0} seconds  {2:##,###;;0} threads",
                    Thread.VolatileRead(ref counters), sp.Elapsed.TotalSeconds, maxWorkerThreads - workerThreads);
            }
        }

    }

    public class Company
    {
        public string Id { get; set; }
        public string ExternalId { get; set; }
        public string Name { get; set; }

        public string Phone { get; set; }
        public string Fax { get; set; }
    }
}
