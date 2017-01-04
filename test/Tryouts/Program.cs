using System;
using System.Threading.Tasks;
using FastTests.Voron;
using StressTests;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            //Parallel.For(0, 100, i =>
            //{
            //    Console.WriteLine(i);
            //    using (var a = new SlowTests.Tests.Sorting.AlphaNumericSorting())
            //    {
            //        a.random_words_using_document_query_async().Wait();
            //    }
            //});

            using (var a = new FastTests.Server.Documents.Replication.ReplicationConflictsTests())
            {
                a.Conflict_then_patching_query_will_return_409_and_conflict_data().Wait();
            }
        }
    }


}

