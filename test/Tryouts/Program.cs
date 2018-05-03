using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using SlowTests.Client.Attachments;
using SlowTests.Client.Counters;
using SlowTests.Tests.Sorting;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace Tryouts
{
    public static class Program
    {

        public static void Main(string[] args)
        {
            using (var tests = new CountersCrudSingleNode())
            {

                tests.MultiGetCounters();

                tests.IncrementCounter();
                tests.DeleteCounter();
                tests.GetCounterValue();

            }

            using (var tests = new CountersCrudMultipuleNodes())
            {

                tests.IncrementCounter().Wait();

            }

            using (var tests = new CountersInMetadata())
            {

                tests.ConflictsInMetadata().Wait();
                tests.IncrementAndDeleteShouldChangeDocumentMetadata();


                Console.WriteLine("all good");
                Console.ReadKey();
            }
        }
    }
}
