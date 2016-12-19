// -----------------------------------------------------------------------
//  <copyright file="AggressiveCacheBeingIgnored.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class AggresiveCacheLazyIgnored : RavenTestBase
    {
        [Fact]
        public void CanAggressivelyCacheLazyLoads()
        {
            using (var server = GetNewServer())
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8079",
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.FailImmediately,
                    ShouldAggressiveCacheTrackChanges = false
                }
            }.Initialize())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        Id = "12345678",
                        FirstName = "Name",
                        LastName = "SurName",
                        LastDate = new DateTime(2016, 1, 1),
                    });
                    session.SaveChanges();
                }

                new SimpleIndex().Execute(store);

                WaitForAllRequestsToComplete(server);
                WaitForIndexing(store);

                server.Server.ResetNumberOfRequests();

                // prewarm query
                LazyLoad(store);
                // wait longer as aggressive cache time
                SystemTime.UtcDateTime = () => DateTime.UtcNow.AddMinutes(1);
                try
                {

                    // use aggresive cache
                    for (var i = 0; i < 5; i++)
                    {
                        LazyLoad(store);
                    }

                }
                finally
                {
                    SystemTime.UtcDateTime = null;
                }

                WaitForAllRequestsToComplete(server);

                // server should be requested twice
                Assert.Equal(2, server.Server.NumberOfRequests);
            }
        }

        private static void LazyLoad(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(1)))
                {
                    var employeeLoad = session.Advanced.Lazily.Load<Employee>("12345678");
                    var employeQuery = session.Query<EmployeeView, SimpleIndex>().Where(x => x.Ident == "12345678").ProjectFromIndexFieldsInto<EmployeeView>().Lazily();

                    Assert.Equal("12345678", employeeLoad.Value.Id);
                    Assert.Equal(1, employeQuery.Value.Count());
                    Assert.Equal("12345678", employeQuery.Value.First().Ident);
                }
            }
        }

        public class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public DateTime LastDate { get; set; }
        }

        public class EmployeeView
        {
            public string Ident { get; set; }
            public string Name { get; set; }
        }

        public class SimpleIndex : AbstractIndexCreationTask<Employee, EmployeeView>
        {
            public SimpleIndex()
            {
                Map = employees => from employee in employees
                                   select new
                                   {
                                       Ident = employee.Id,
                                       Name = employee.FirstName + " " + employee.LastName
                                   };
                StoreAllFields(Raven.Abstractions.Indexing.FieldStorage.Yes);
            }
        }
    }
}