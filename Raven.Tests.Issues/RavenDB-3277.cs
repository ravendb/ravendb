using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3277 : RavenTestBase
    {
        [Fact]
        public void SortAllNumericalFields()
        {
            using (var store = NewDocumentStore())
            {
                store.Initialize();
                new AutoSorted_Index().Execute(store);
                new AutoSorted_Index1().Execute(store);
                new AutoSorted_Index2().Execute(store);
                var indexDefinition = store.DatabaseCommands.GetIndex("AutoSorted/Index");
                var indexDefinition1 = store.DatabaseCommands.GetIndex("AutoSorted/Index1");
                var indexDefinition2 = store.DatabaseCommands.GetIndex("AutoSorted/Index2");
                Assert.Equal(SortOptions.Int, indexDefinition.SortOptions["IntegerAge"]);
                Assert.Equal(SortOptions.Long, indexDefinition1.SortOptions["IntegerAge"]);
                Assert.Equal(SortOptions.Int, indexDefinition2.SortOptions["IntegerAge"]);
            }

        }

        public class AutoSorted
        {
            public string StringName { get; set; }
            public int IntegerAge { get; set; }
            public double DoubleSalary { get; set; }
        }

        public class AutoSorted_Index : AbstractIndexCreationTask<AutoSorted>
        {
            public AutoSorted_Index()
            {
                Map = docs => from doc in docs
                    select new {doc.StringName, doc.IntegerAge, doc.DoubleSalary};

            }
        }
        public class AutoSorted_Index1 : AbstractIndexCreationTask<AutoSorted>
        {
            public AutoSorted_Index1()
            {
                Map = docs => from doc in docs
                              select new { doc.StringName, doc.IntegerAge, doc.DoubleSalary };

                Sort("IntegerAge", SortOptions.Long);

            }
        }

        public class AutoSorted_Index2 : AbstractIndexCreationTask<AutoSorted>
        {
            public AutoSorted_Index2()
            {
                Map = docs => from doc in docs
                    where doc.DoubleSalary > 10
                    select new {doc.StringName, doc.IntegerAge, doc.DoubleSalary};
            }
        }
    }
}

