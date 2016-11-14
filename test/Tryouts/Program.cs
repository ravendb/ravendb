using System;
using FastTests.Server.Documents.Replication;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var store = new DocumentReplication())
                {
                    store.CanReplicateDocument().Wait();
                }
            }
        }
    }

}

