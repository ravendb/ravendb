﻿using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using SlowTests.Tests.Linq;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDBBookJavaScriptIndexes : RavenTestBase
    {

        [Fact]
        public void RegionsByTerritoriesNames()
        {
            ExecuteIndexAndWaitForUserToContinueTheTest<MultiField>();            
            //ExecuteIndexAndWaitForUserToContinueTheTest<MultiMap>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<OrdersTotal>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<EmployeeByName>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<EmployeeByHiredAt>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<EmployeeIndex>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<RegionsByTerritoriesName>();
        }

        public void ExecuteIndexAndWaitForUserToContinueTheTest<T>() where T:AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                new T().Execute(store);
                WaitForUserToContinueTheTest(store);
            }
        }
        private void CreateNorthwindDatabase(DocumentStore store)
        {
            store.Maintenance.Send(new CreateSampleDataOperation());
        }

        private class OrdersTotal : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "OrdersTotal",
                    Maps = new HashSet<string>
                    {
@"map('Orders', function (o){ 
function Sum(total, ol)
{
    return total + (ol.Quantity * ol.PricePerUnit) * (1- ol.Discount);
}
    return { 
             Employee: o.Employee  ,
             Company: o.Company,
             Total: o.Lines.reduce(Sum,0)
           };                            
})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class MultiField : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "MultiField",
                    Maps = new HashSet<string>
                    {
                        @"map('Companies', function (c){ return {Query : [c.ExternalId, c.Name, c.Contact.Name] } })"
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class MultiMap : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "MultiMap",
                    Maps = new HashSet<string>
                    {
                        @"map('Employees', function (e){ return {Name: e.FirstName + ' ' + e.LastName}})",
                        @"map('Companies', function (c){ return {Name: c.Contact.Name}})",
                        @"map('Suppliers', function (s){ return {Name: s.Contact.Name}})"
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class EmployeeByName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "EmployeeByName",
                    Maps = new HashSet<string>
                    {
                        @"map('Employees', function (e){ 
                            return { Name: e.FirstName + ' ' + e.LastName };                            
                        })",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
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
