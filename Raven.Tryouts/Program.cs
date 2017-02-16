using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Changes;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Core.Replication;
using Raven.Tests.Issues;
using Xunit;

namespace Tryouts
{

    public class ReplicationTest : RavenReplicationCoreTest
    {
        
        public void can_reset_index_with_replication()
        {
            
        }
    }
    public class Program
    {
        public class User
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        public static void Main(string[] args)
        {
            int choice = 0;
            TestASync();
            //if (args.Length == 1)
            //{
            //    int.TryParse(args[0], out choice);
            //}
            //switch (choice)
            //{
            //    case 0:
            //        TestSync();
            //        break;
            //    case 1:
            //        TestASync();
            //        break;
            //    case 2:
            //        TestASyncSafer();
            //        break;
            //    default:
            //        TestSync();
            //        break;
            //}
        }

        private static void TestASyncSafer()
        {
            using (var masterStore = new DocumentStore()
            {
                ConnectionStringName = "RavenDB"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();


                while (sp.ElapsedMilliseconds < 10 * 60 * 1000)
                {
                    Console.WriteLine(sp.Elapsed);

                    try
                    {
                        using (var session = masterStore.OpenAsyncSession())
                        {
                            var loadAsyncTask = session.LoadAsync<RavenJObject>("products/1");
                            Task.WaitAll(loadAsyncTask);
                            var res = loadAsyncTask.Result;
                        }
                    }
                    catch (Exception ex)
                    {
                        Debugger.Launch();
                    }

                    Thread.Sleep(1000);
                }
            }
        }

        private static void TestASync()
        {
            using (var masterStore = new DocumentStore()
            {
                //ConnectionStringName = "RavenDB"
                Url="http://localhost:8080",
                DefaultDatabase = "Rep1"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();


                while (true)
                {
                    Console.Write(sp.Elapsed);

                    try
                    {
                        using (var session = masterStore.OpenAsyncSession())
                        {
                            var res = session.LoadAsync<RavenJObject>("docs/1").Result;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.Write(" Error");
                    }
                    Console.WriteLine();

                    Thread.Sleep(1000);
                }
            }
        }

        private static void TestSync()
        {
            using (var masterStore = new DocumentStore()
            {
                ConnectionStringName = "RavenDB"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();


                while (sp.ElapsedMilliseconds < 10*60*1000)
                {
                    Console.WriteLine(sp.Elapsed);

                    try
                    {
                        using (var session = masterStore.OpenSession())
                        {
                            var res = session.Load<dynamic>("docs/1");
                        }
                    }
                    catch (Exception ex)
                    {
                        Debugger.Launch();
                    }

                    Thread.Sleep(1000);
                }
            }
        }
    }
}