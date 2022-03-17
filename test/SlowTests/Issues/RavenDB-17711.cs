using System;
using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17711 : RavenTestBase
    {
        public RavenDB_17711(ITestOutputHelper output) : base(output)
        {
        }

        private class Task
        {
            public object At;
        }
        
        [Fact]
        public void ThreeDigitsFractionalDateParsingShouldWorkProperly_AutoIndex()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                s.Store(new Task{At = "2021-12-14T11:22:28.322Z"});
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                object date = new DateTime(2021, 12, 14, 11, 22, 28, 322, DateTimeKind.Utc);

                Assert.True(s.Query<Task>().Any(t => t.At == date));
            }
        }

        private class Index : AbstractIndexCreationTask<Task>
        {
            public Index()
            {
                Map = tasks => from t in tasks select new { t.At };
            }
        }
          
        [Fact]
        public void ThreeDigitsFractionalDateParsingShouldWorkProperly_StaticIndex()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                s.Store(new Task{At = "2021-12-14T11:22:28.322Z"});
                s.SaveChanges();
            }
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var s = store.OpenSession())
            {
                object date = new DateTime(2021, 12, 14, 11, 22, 28, 322, DateTimeKind.Utc);

                Assert.True(s.Query<Task, Index>().Any(t => t.At == date));
            }
        }
    }
}
