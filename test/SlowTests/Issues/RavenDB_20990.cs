using System;
using System.Collections.Generic;
using FastTests;
using Orders;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.Exceptions;

namespace SlowTests.Issues
{
    public class RavenDB_20990 : RavenTestBase
    {
        public RavenDB_20990(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSearchWithProximityZero()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var session = store.OpenSession())
                {
                    var employee9A = session.Load<Employee>("employees/9-A");
                    employee9A.Notes[0] = "Anne has a BA degree in English from St. Lawrence College. She has fluent French.";
                    session.Store(employee9A);
                    session.SaveChanges();
                }

                // Terms that are maximum 5 terms apart
                using (var session = store.OpenSession())
                {
                    List<Employee> employeesWithProximity5 = session.Advanced
                        .DocumentQuery<Employee>()
                        .Search(x => x.Notes, "fluent french")
                        .Proximity(5)
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal(4, employeesWithProximity5.Count);
                }

                // Terms that are 0 terms apart
                // Results will contain a single word that was not tokenized to a term in between
                using (var session = store.OpenSession())
                {
                    List<Employee> employeesWithProximity0 = session.Advanced
                        .DocumentQuery<Employee>()
                        .Search(x => x.Notes,"fluent french")
                        .Proximity(0)
                        .ToList();

                    Assert.Equal(3, employeesWithProximity0.Count);
                    
                    Assert.Equal(employeesWithProximity0[0].Id, "employees/2-A");
                    Assert.Equal(employeesWithProximity0[1].Id, "employees/5-A");
                    Assert.Equal(employeesWithProximity0[2].Id, "employees/9-A");

                    Assert.Contains("fluent in French", employeesWithProximity0[0].Notes[0]);
                    Assert.Contains("fluent in French", employeesWithProximity0[1].Notes[0]);
                    Assert.Contains("fluent French", employeesWithProximity0[2].Notes[0]);
                }

                // Test that are 0 terms apart
                // Consecutive results only
                using (var session = store.OpenSession())
                {
                    var employee2A = session.Load<Employee>("employees/2-A");
                    employee2A.Notes[0] = "Andrew knows fluent Belgian French.";
                    session.Store(employee2A);
                    
                    var employee5A = session.Load<Employee>("employees/5-A");
                    employee5A.Notes[0] = "Steven knows fluent Canadian French.";
                    session.Store(employee5A);
                    
                    session.SaveChanges();
                    
                    List<Employee> employeesWithProximity0 = session.Advanced
                        .DocumentQuery<Employee>()
                        .Search(x => x.Notes,"fluent french")
                        .Proximity(0)
                        .WaitForNonStaleResults()
                        .ToList();
                    
                    Assert.Equal(1, employeesWithProximity0.Count);
                    Assert.Equal(employeesWithProximity0[0].Id, "employees/9-A");
                }
            }
        }

        [Fact]
        public void CannotSearchWithNegativeProximity()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var msg = Assert.Throws<ArgumentOutOfRangeException>(() =>
                        session.Advanced
                            .DocumentQuery<Employee>()
                            .Search(x => x.Notes, "fluent french")
                            .Proximity(-1)
                            .ToList());

                    Assert.Contains("Proximity distance must be a number greater than or equal to 0", msg.Message);
                }
            }
        }
        
        [Fact]
        public void CannotUseProximityAfterWhereClause()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var msg = Assert.Throws<InvalidOperationException>(() =>
                        session.Advanced
                            .DocumentQuery<Employee>()
                            .WhereEquals("Notes", "fluent french")
                            .Proximity(1)
                            .ToList());

                    Assert.Contains("Proximity can only be used right after Search clause", msg.Message);
                }
            }
        }
        
        [Fact]
        public void CannotUseProximityWithSingleTerm()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var msg = Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced
                            .DocumentQuery<Employee>()
                            .Search(x => x.Notes, "fluent")
                            .Proximity(1)
                            .ToList());

                    Assert.Contains("Proximity search works only on multiple search terms", msg.Message);
                }
            }
        }
        
        [Fact]
        public void CannotUseProximityWithWildcards()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var msg = Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced
                            .DocumentQuery<Employee>()
                            .Search(x => x.Notes, "*luent frenc*")
                            .Proximity(1)
                            .ToList());

                    Assert.Contains("Proximity search works only on simple string terms, not wildcard", msg.Message);
                }
            }
        }
    }
}
