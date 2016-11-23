using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class ScriptedPatchBug : RavenTestBase
    {
        private class Index1 : AbstractIndexCreationTask<Compound>
        {
            public Index1()
            {
                Map = compounds => from c in compounds
                                   select new
                                   {
                                       c.Value
                                   };
            }
        }

        [Fact(Skip = "Missing feature: Tasks (operations) and their results")]
        public void Test()
        {
            using (var store = GetDocumentStore())
            {
                store.JsonRequestFactory.RequestTimeout = TimeSpan.FromHours(1);

                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    var compound = new Compound
                    {
                        Id = "compound/1",
                        ReferenceNumber = "b",
                        Compounds = { new Compound { ReferenceNumber = "c" } } // Comment this line out to make the test work
                    };

                    session.Store(compound);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                const string script = @"if (this.ReferenceNumber == 'a'){ this.ReferenceNumber = 'Aa'; }";

                var operation = store.DatabaseCommands.UpdateByIndex(
                    new Index1().IndexName,
                    new IndexQuery { Query = string.Empty },
                    new PatchRequest { Script = script },
                    new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.MaxValue, RetrieveDetails = true });

                var patchResult = operation.WaitForCompletion(TimeSpan.FromSeconds(15));
                Assert.False(patchResult.ToString().Contains("Patched"));
            }
        }

        private class Compound
        {
            public Compound()
            {
                Compounds = new List<Compound>();
            }
            public IList<Compound> Compounds { get; set; }

            public string Id { get; set; }
            public string ReferenceNumber { get; set; }
            public decimal Value { get; set; } // You can also comment this line out to make the test work

        }
    }
}
