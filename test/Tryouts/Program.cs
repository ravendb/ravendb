using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using FastTests.Server.Basic;
using FastTests.Server.Documents.Alerts;
using FastTests.Server.Documents.Patching;
using FastTests.Server.Documents.Replication;
using FastTests.Server.Documents.SqlReplication;
using Raven.Client.Document;
using SlowTests.Core.Commands;
using Sparrow.Json;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public int Score;
            public string Name;
            public DateTime CreatedAt;
            public Dictionary<string, string> CustomProperties = new Dictionary<string, string>();
        }

        

        private static readonly char[] _buffer = new char[6];
        private static string RandomName(Random rand)
        {
            _buffer[0] = (char)rand.Next(65,91);
            for (int i = 1; i < 6; i++)
            {
                _buffer[i] = (char) rand.Next(97, 123);
            }
            return new string(_buffer);
        }


        private static readonly char[] _buffer2 = new char[600];
        private static string RandomStr(Random rand)
        {
            for (int i = 0; i < 600; i++)
            {
                _buffer2[i] = (char)rand.Next(97, 123);
            }
            return new string(_buffer2,0, 600);
        }

        static void Main(string[] args)
        {
            var sb = new StringBuilder();
            for (int i = 0; i < 700; i++)
            {
                sb.Append(
                    @"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum");
            }
            var lorem = sb.ToString();
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "bench"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();
                using (var bulk = store.BulkInsert())
                {
                    var rand = new Random();
                    for (int i = 0; i < 100*1000; i++)
                    {
                        var entity = new User
                        {
                            CreatedAt = DateTime.Today.AddDays(rand.Next(356)),
                            Score = rand.Next(0, 5000),
                            Name = RandomName(rand),
                        };
                        for (int j = 0; j < rand.Next(150,1500); j++)
                        {
                            entity.CustomProperties[RandomName(rand)] = RandomStr(rand);
                        }
                        bulk.Store(entity);
                    }
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

