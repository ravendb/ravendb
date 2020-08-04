using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15460 : RavenTestBase
    {
        public RavenDB_15460(ITestOutputHelper output) : base(output)
        {
        }

        private class Companies_Counts_ByName : AbstractIndexCreationTask<Company, Companies_Counts_ByName.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public long Count { get; set; }
            }

            public Companies_Counts_ByName()
            {
                Map = companies => from company in companies
                                   select new
                                   {
                                       company.Name,
                                       Count = 1
                                   };

                Reduce = results => from result in results
                                    group result by result.Name into g
                                    select new
                                    {
                                        Name = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };

                Priority = IndexPriority.High;
            }
        }

        private static IEnumerable<TType> GetAllInstancesOfType<TType>(Assembly assembly, Type t)
        {
            foreach (var type in assembly.GetTypes()
                .Where(x =>
                x == t &&
                x.GetTypeInfo().IsClass &&
                x.GetTypeInfo().IsAbstract == false &&
                x.GetTypeInfo().IsSubclassOf(typeof(TType))))
            {
                yield return (TType)Activator.CreateInstance(type);
            }
        }

        [Fact]
        public async Task Setting_Index_Priority_Should_Work()
        {
            using (var store = GetDocumentStore())
            {
                var type = typeof(Companies_Counts_ByName);
                var index = GetAllInstancesOfType<AbstractIndexCreationTask>(type.Assembly, type).Single();
                await IndexCreation.CreateIndexesAsync(new List<AbstractIndexCreationTask> { index }, store);

                var indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(index.IndexName));
                var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

                Assert.Equal(IndexPriority.High, indexDefinition.Priority);
                Assert.Equal(IndexPriority.High, indexStats.Priority);

                index.Priority = IndexPriority.Low;

                await IndexCreation.CreateIndexesAsync(new List<AbstractIndexCreationTask> { index }, store);

                indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(index.IndexName));
                indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

                Assert.Equal(IndexPriority.Low, indexDefinition.Priority);
                Assert.Equal(IndexPriority.Low, indexStats.Priority);

                index.Priority = IndexPriority.High;
                index.Execute(store);

                indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(index.IndexName));
                indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

                Assert.Equal(IndexPriority.High, indexDefinition.Priority);
                Assert.Equal(IndexPriority.High, indexStats.Priority);

                index.Priority = IndexPriority.Low;
                store.ExecuteIndex(index);

                indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(index.IndexName));
                indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

                Assert.Equal(IndexPriority.Low, indexDefinition.Priority);
                Assert.Equal(IndexPriority.Low, indexStats.Priority);

                indexDefinition = index.CreateIndexDefinition();
                await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));

                indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(index.IndexName));
                indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

                Assert.Equal(IndexPriority.High, indexDefinition.Priority);
                Assert.Equal(IndexPriority.High, indexStats.Priority);
            }
        }
    }
}
