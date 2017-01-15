using System;
using FastTests.Blittable.BlittableJsonWriterTests;
using FastTests.Client.Indexing;
using FastTests.Client.Queries;
using FastTests.Server.Basic;
using FastTests.Server.Documents.Indexing.MapReduce;
using FastTests.Server.Documents.Indexing.Static;
using FastTests.Server.Replication;
using FastTests.Utils;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var a = new DocumentReplication())
            {
                a.CanReplicateDocumentDeletion();
            }
            GC.Collect(2);
            GC.WaitForPendingFinalizers();
        }
    }
}

