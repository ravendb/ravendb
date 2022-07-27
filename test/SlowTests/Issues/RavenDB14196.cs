using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB14196 : RavenTestBase
    {
        public RavenDB14196(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanHandleIndexWithIntMethodCall()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DateIndex());
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument123 {Geburtsdatum = new DateTime(2010, 01, 01)});
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);

                Assert.Null(Indexes.WaitForIndexingErrors(store, errorsShouldExists: false));
            }
        }
    }

    public class DateIndex : AbstractIndexCreationTask<TestDocument123, DateIndex.Result>
    {
        public class Result
        {
            public DateTime NewDate { get; set; }
        }

        public DateIndex()
        {
            Map = documents => from document in documents
                //let newDate = document.Geburtsdatum.AddYears((int)document.Offset) // working
                let newDate = document.Geburtsdatum.AddYears(document.Offset) // not working
                select new {NewDate = newDate};
            Store(r => r.NewDate, FieldStorage.Yes);
        }
    }

    public class TestDocument123
    {
        public DateTime Geburtsdatum { get; set; }
        public int Offset { get; set; }
    }
}
