using System;
using System.Diagnostics;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                var sp = Stopwatch.StartNew();
                using (var x = new FastTests.Server.Documents.Replication.AutomaticConflictResolution())
                {
                    x.Resolve_to_latest_version_tombstone_is_latest_the_incoming_document_is_replicated();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

