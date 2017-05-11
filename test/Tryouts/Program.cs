using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using FastTests.Client;
using FastTests.Server.Documents.Queries;
using FastTests.Server.Replication;
using SlowTests.Client.Attachments;
using SlowTests.Core.Session;
using SlowTests.SlowTests.Issues;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var a = new RavenDB_1280_ReOpen())
                {
                    a.Can_Index_With_Missing_LoadDocument_References();
                }
            }
        }
    }
}