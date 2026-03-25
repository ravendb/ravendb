using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_14903 : RavenTestBase
    {
        public RavenDB_14903(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void ShouldThrowOnOrderByInMap()
        {
            using (var store = GetDocumentStore())
            {
                Assert.Throws<IndexCompilationException>(() => new SystemMetrics_ByDateDescending().Execute(store));
                Assert.Throws<IndexCompilationException>(() => new SystemMetrics_ByDate().Execute(store));
            }
        }

        private class SystemMetrics
        {
            public DateTimeOffset Created { get; set; }
        }
        private class SystemMetrics_ByDateDescending : AbstractIndexCreationTask<SystemMetrics>
        {
            public SystemMetrics_ByDateDescending()
            {
                Map = metrics => from metric in metrics
                                 orderby metric.Created descending
                                 select new
                                 {
                                     metric.Created
                                 };
            }
        }
        private class SystemMetrics_ByDate : AbstractIndexCreationTask<SystemMetrics>
        {
            public SystemMetrics_ByDate()
            {
                Map = metrics => from metric in metrics
                                 orderby metric.Created
                                 select new
                                 {
                                     metric.Created
                                 };
            }
        }
    }
}
