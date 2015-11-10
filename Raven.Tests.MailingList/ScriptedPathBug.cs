using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class ScriptedPatchBug : RavenTest
    {
        [Fact]
        public void Test()
        {
            using (var store = NewDocumentStore())
            {
                store.Initialize();
                store.JsonRequestFactory.RequestTimeout = TimeSpan.FromHours(1);
                new RavenDocumentsByEntityName().Execute(store);

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

                var script = @"if (this.ReferenceNumber == 'a'){ this.ReferenceNumber = 'Aa'; }";

                var operation = store.DatabaseCommands.UpdateByIndex(
                    "Raven/DocumentsByEntityName",
                    new IndexQuery { Query = "Tag:Compounds" },
                    new ScriptedPatchRequest { Script = script },
                    new BulkOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.MaxValue, RetrieveDetails = true });

                var patchResult = operation.WaitForCompletion();
                Assert.False(patchResult.ToString().Contains("Patched"));
            }
        }



        public class Compound
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
