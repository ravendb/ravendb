using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class NicolasGarfinkiel : RavenTestBase
    {
        public NicolasGarfinkiel(ITestOutputHelper output) : base(output)
        {
        }

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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void CanQueryDynamically(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {  new IndexDefinition()
                {
                    Name = "Foos/TestIndex",
                    Maps =
                    {
                        @"from doc in docs.LaboratoryTrials
            select new
            {
                _ = doc.Patient.IdCards.Select((Func<dynamic,dynamic>)(x => new Field(x.Key, x.Value, Field.Store.NO, Field.Index.ANALYZED_NO_NORMS)))
            }"
                    }
                }}));

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
                        .WaitForNonStaleResults(TimeSpan.FromHours(1))
                        .WhereEquals("Read", "Yes")
                        .ToList();
                    Assert.NotEmpty(laboratoryTrials);
                }
            }
        }
    }
}
