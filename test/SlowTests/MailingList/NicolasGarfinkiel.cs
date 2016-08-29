using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Indexing;
using Xunit;

namespace SlowTests.MailingList
{
    public class NicolasGarfinkiel : RavenTestBase
    {
        private class LaboratoryTrial
        {
            public DateTimeOffset CreatedDate { get; set; }

            public DateTimeOffset DeliveryDate { get; set; }

            public DateTimeOffset LastModifiedDate { get; set; }

            public string Satus { get; set; }

            public Patient Patient { get; set; }

        }

        private class Patient
        {

            public string Firstname { get; set; }

            public string Lastname { get; set; }

            public Dictionary<string, string> IdCards { get; set; }

        }

        [Fact]
        public void CanQueryDynamically()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("Foos/TestIndex", new IndexDefinition()
                {
                    Maps =
                    {
                        @"from doc in docs.LaboratoryTrials
            select new
            {
                _ = doc.Patient.IdCards.Select((Func<dynamic,dynamic>)(x => new Field(x.Key, x.Value, Field.Store.NO, Field.Index.ANALYZED_NO_NORMS)))
            }"
                    }
                }, true);

                using (var session = store.OpenSession())
                {
                    session.Store(new LaboratoryTrial
                    {
                        CreatedDate = DateTimeOffset.Now,
                        DeliveryDate = DateTimeOffset.Now,
                        LastModifiedDate = DateTimeOffset.Now,
                        Satus = "H",
                        Patient = new Patient
                        {
                            Firstname = "Ha",
                            Lastname = "Dr",
                            IdCards = new Dictionary<string, string>
                            {
                                {"Read", "Yes"},
                            }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var laboratoryTrials = session.Advanced.DocumentQuery<LaboratoryTrial>("Foos/TestIndex")
                        .WaitForNonStaleResultsAsOfLastWrite(TimeSpan.FromHours(1))
                        .WhereEquals("Read", "Yes")
                        .ToList();
                    Assert.NotEmpty(laboratoryTrials);
                }
            }
        }
    }
}
