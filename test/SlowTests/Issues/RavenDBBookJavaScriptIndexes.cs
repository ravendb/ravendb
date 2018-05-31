using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDBBookJavaScriptIndexes : RavenTestBase
    {
        [Fact(Skip = "Playground test")]
        public void EmployeeIndexTest()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                new EmployeeIndex().Execute(store);
                WaitForUserToContinueTheTest(store);
            }
        }

        [Fact(Skip = "Playground test")]
        public void EmployeeByHiredAtTest()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                new EmployeeByHiredAt().Execute(store);
                WaitForUserToContinueTheTest(store);
            }
        }

        [Fact(Skip = "Playground test")]
        public void RegionsByTerritoriesNames()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                new RegionsByTerritoriesName().Execute(store);
                WaitForUserToContinueTheTest(store);
            }
        }

        private void CreateNorthwindDatabase(DocumentStore store)
        {
            store.Maintenance.Send(new CreateSampleDataOperation());
        }

        private class RegionsByTerritoriesName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "RegionsByTerritoriesName",
                    Maps = new HashSet<string>
                    {
                        @"map('Regions', function (r){ 
                            return { Name: r.Territories.map(x=>x.Name) };                            
                        })",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }
        private class EmployeeIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "EmployeeIndex",
                    Maps = new HashSet<string>
                    {
                        @"map('Employees', function (e){ return e})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class EmployeeByHiredAt : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "EmployeeByHiredAt",
                    Maps = new HashSet<string>
                    {
                        @"
                            map('Employees', function (e){var date = new Date(e.HiredAt); return {HiredAt: date, 'HiredAt.Year':date.getYear()}})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }
    }
}
