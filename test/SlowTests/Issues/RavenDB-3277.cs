using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3277 : RavenTestBase
    {
        [Fact]
        public void SortAllNumericalFields()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                new AutoSorted_Index().Execute(store);
                new AutoSorted_Index1().Execute(store);
                new AutoSorted_Index2().Execute(store);
                var indexDefinition = store.Admin.Send(new GetIndexOperation("AutoSorted/Index"));
                var indexDefinition1 = store.Admin.Send(new GetIndexOperation("AutoSorted/Index1"));
                var indexDefinition2 = store.Admin.Send(new GetIndexOperation("AutoSorted/Index2"));
                Assert.Equal(SortOptions.Numeric, indexDefinition.Fields["IntegerAge"].Sort);
                Assert.Equal(SortOptions.Numeric, indexDefinition1.Fields["IntegerAge"].Sort);
                Assert.Equal(SortOptions.Numeric, indexDefinition2.Fields["IntegerAge"].Sort);
            }

        }

        private class AutoSorted
        {
            public string StringName { get; set; }
            public int IntegerAge { get; set; }
            public double DoubleSalary { get; set; }
        }

        private class AutoSorted_Index : AbstractIndexCreationTask<AutoSorted>
        {
            public AutoSorted_Index()
            {
                Map = docs => from doc in docs
                              select new { doc.StringName, doc.IntegerAge, doc.DoubleSalary };

            }
        }

        private class AutoSorted_Index1 : AbstractIndexCreationTask<AutoSorted>
        {
            public AutoSorted_Index1()
            {
                Map = docs => from doc in docs
                              select new { doc.StringName, doc.IntegerAge, doc.DoubleSalary };

                Sort("IntegerAge", SortOptions.Numeric);

            }
        }

        private class AutoSorted_Index2 : AbstractIndexCreationTask<AutoSorted>
        {
            public AutoSorted_Index2()
            {
                Map = docs => from doc in docs
                              where doc.DoubleSalary > 10
                              select new { doc.StringName, doc.IntegerAge, doc.DoubleSalary };
            }
        }
    }
}

