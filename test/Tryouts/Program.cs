using System;
using System.Diagnostics;
using Raven.Client.Document;
using Raven.Client.Smuggler;

namespace Tryouts
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            using (var store = new DocumentStore {Url = "http://localhost:8080", DefaultDatabase = "FreeDB"})
            {
                store.Initialize();
                var sw = new Stopwatch();
                sw.Start();
                var task = store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), @"c:\dumps\freedb.raven.dump");
                task.Wait();
                sw.Stop();
                Console.WriteLine(sw.ElapsedMilliseconds);
            }
        }
    }
}