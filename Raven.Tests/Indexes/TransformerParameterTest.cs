using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Indexes
{
    public class TransformerParameterTest : RavenTestBase
    {
        public class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
        }

        public class EmployeeWithParam
        {
            public Employee Employee { get; set; }
            public string Param { get; set; }
        }

        public class Employees_ByFirstName : AbstractIndexCreationTask<Employee>
        {
            public Employees_ByFirstName()
            {
                Map = employees => from employee in employees
                    select new
                    {
                        employee.Id,
                        employee.FirstName,
                    };
            }
        }

        public class EmployeeTransformerTest : AbstractTransformerCreationTask<Employee>
        {
            public EmployeeTransformerTest()
            {
                TransformResults = items => from item in items
                    let param = ParameterOrDefault("param", string.Empty).Value<string>()
                    select new
                    {
                        Employee = item,
                        Param = param,
                    };
            }
        }

        [Fact]
        public void UnescapedTransformerParameterTest()
        {
            using (DocumentStore store = NewRemoteDocumentStore())
            {
                new Employees_ByFirstName().Execute(store);
                new EmployeeTransformerTest().Execute(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(new Employee { Id = "1-Alice", FirstName = "Alice", });
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession())
                {
                    var results = session.Query<Employee, Employees_ByFirstName>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .TransformWith<EmployeeWithParam>("EmployeeTransformerTest")
                        .AddTransformerParameter("param","foo+bar%20baz")
                        .ToArray();

                    Assert.Equal("foo+bar%20baz", results.First().Param);
                }

                using (IDocumentSession session = store.OpenSession())
                {
                    var results = session.Query<Employee, Employees_ByFirstName>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .TransformWith<EmployeeWithParam>("EmployeeTransformerTest")
                        .AddTransformerParameter("param", "foo+bar baz")
                        .ToArray();

                    Assert.Equal("foo+bar baz", results.First().Param);
                }
            }
        }
    }
}