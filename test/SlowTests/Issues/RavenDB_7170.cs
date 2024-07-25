using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents.Indexes.Static;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_7170 : NoDisposalNeeded
    {
        public RavenDB_7170(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Indexing_func_must_return_anonymous_object()
        {
            var ex = Assert.Throws<IndexCompilationException>(() => IndexCompiler.Compile(new Bad_index_1().CreateIndexDefinition(), 0));
            Assert.Contains("Indexing function must return an anonymous object", ex.Message);

            ex = Assert.Throws<IndexCompilationException>(() => IndexCompiler.Compile(new Bad_index_2().CreateIndexDefinition(), 0));
            Assert.Contains("Indexing function must return an anonymous object", ex.Message);
        }

        private class Bad_index_1 : AbstractIndexCreationTask<DroneStateSnapshoot>
        {
            public Bad_index_1()
            {
                Map = snapshots => snapshots.Select(x => x.A);
            }
        }

        private class Bad_index_2 : AbstractIndexCreationTask<DroneStateSnapshoot>
        {
            public Bad_index_2()
            {
                Map = snapshots => snapshots.SelectMany(x => x.ClickActions, (snapshoot, x) => x);
            }
        }

        private class DroneStateSnapshoot
        {
            public string A { get; set; }
            public IList<ClickAction> ClickActions { get; set; }
        }

        private class ClickAction
        {
            public string ContactId { get; set; }
            public string CreativeId { get; set; }
            public DateTime Date { get; set; }
        }

        private class ReduceResult
        {
            public string CreativeId { get; set; }
            public int Count { get; set; }
        }
    }
}
