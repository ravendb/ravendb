using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11878 : RavenTestBase
    {
        public RavenDB_11878(ITestOutputHelper output) : base(output)
        {
        }

        private class AttachmentIndex : AbstractIndexCreationTask<Employee>
        {
            public class Result
            {
                public List<string> AttachmentNames { get; set; }
            }

            public AttachmentIndex()
            {
                Map = employees => from e in employees
                                   let attachments = AttachmentsFor(e)
                                   select new
                                   {
                                       AttachmentNames = attachments.Select(x => x.Name)
                                   };
            }
        }

        private class CounterIndex : AbstractIndexCreationTask<Employee>
        {
            public class Result
            {
                public List<string> CounterNames { get; set; }
            }

            public CounterIndex()
            {
                Map = employees => from e in employees
                                   let counterNames = CounterNamesFor(e)
                                   select new
                                   {
                                       CounterNames = counterNames
                                   };
            }
        }

        [Fact]
        public void SupportAttachmentsForInIndex()
        {
            using (var store = GetDocumentStore())
            {
                new AttachmentIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "John",
                        LastName = "Doe"
                    }, "employees/1");

                    session.Store(new Employee
                    {
                        FirstName = "Bob",
                        LastName = "Doe"
                    }, "employees/2");

                    session.Store(new Employee
                    {
                        FirstName = "Edward",
                        LastName = "Doe"
                    }, "employees/3");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var employees = session.Query<AttachmentIndex.Result, AttachmentIndex>()
                        .Where(x => x.AttachmentNames.Contains("photo.jpg"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(0, employees.Count);
                }

                using (var session = store.OpenSession())
                {
                    var employee1 = session.Load<Employee>("employees/1");
                    var employee2 = session.Load<Employee>("employees/2");

                    session.Advanced.Attachments.Store(employee1, "photo.jpg", new MemoryStream(), "image/jpeg");
                    session.Advanced.Attachments.Store(employee1, "cv.pdf", new MemoryStream(), "application/pdf");

                    session.Advanced.Attachments.Store(employee2, "photo.jpg", new MemoryStream(), "image/jpeg");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var employees = session.Query<AttachmentIndex.Result, AttachmentIndex>()
                        .Where(x => x.AttachmentNames.Contains("photo.jpg"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(2, employees.Count);
                    Assert.Contains("John", employees.Select(x => x.FirstName));
                    Assert.Contains("Bob", employees.Select(x => x.FirstName));

                    employees = session.Query<AttachmentIndex.Result, AttachmentIndex>()
                        .Where(x => x.AttachmentNames.Contains("cv.pdf"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(1, employees.Count);
                    Assert.Contains("John", employees.Select(x => x.FirstName));
                }

                using (var session = store.OpenSession())
                {
                    var employee1 = session.Load<Employee>("employees/1");

                    session.Advanced.Attachments.Delete(employee1, "photo.jpg");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var employees = session.Query<AttachmentIndex.Result, AttachmentIndex>()
                        .Where(x => x.AttachmentNames.Contains("photo.jpg"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(1, employees.Count);
                    Assert.Contains("Bob", employees.Select(x => x.FirstName));
                }
            }
        }

        [Fact]
        public void SupportCounterNamesForInIndex()
        {
            using (var store = GetDocumentStore())
            {
                new CounterIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "John",
                        LastName = "Doe"
                    }, "employees/1");

                    session.Store(new Employee
                    {
                        FirstName = "Bob",
                        LastName = "Doe"
                    }, "employees/2");

                    session.Store(new Employee
                    {
                        FirstName = "Edward",
                        LastName = "Doe"
                    }, "employees/3");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var employees = session.Query<CounterIndex.Result, CounterIndex>()
                        .Where(x => x.CounterNames.Contains("Likes"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(0, employees.Count);
                }

                using (var session = store.OpenSession())
                {
                    var employee1 = session.Load<Employee>("employees/1");
                    var employee2 = session.Load<Employee>("employees/2");

                    session.CountersFor(employee1).Increment("Likes", 10);
                    session.CountersFor(employee1).Increment("Dislikes", 5);

                    session.CountersFor(employee2).Increment("Likes", 3);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var employees = session.Query<CounterIndex.Result, CounterIndex>()
                        .Where(x => x.CounterNames.Contains("Likes"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(2, employees.Count);
                    Assert.Contains("John", employees.Select(x => x.FirstName));
                    Assert.Contains("Bob", employees.Select(x => x.FirstName));

                    employees = session.Query<CounterIndex.Result, CounterIndex>()
                        .Where(x => x.CounterNames.Contains("Dislikes"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(1, employees.Count);
                    Assert.Contains("John", employees.Select(x => x.FirstName));
                }

                using (var session = store.OpenSession())
                {
                    var employee1 = session.Load<Employee>("employees/1");

                    session.CountersFor(employee1).Delete("Likes");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var employees = session.Query<CounterIndex.Result, CounterIndex>()
                        .Where(x => x.CounterNames.Contains("Likes"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(1, employees.Count);
                    Assert.Contains("Bob", employees.Select(x => x.FirstName));
                }
            }
        }
    }
}
