using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12094 : RavenTestBase
    {
        private class Employee
        {
            public string Id { get; set; }

            public IEnumerable<Skill> Skills { get; set; }

            public DateTime Revision { get; set; }

        }

        private class Skill
        {
            public string Name { get; set; }

            public int Level { get; set; }
        }

        [Fact]
        public void ProjectingDateViaJsShouldHaveSameValueAsSimpleDateProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var employee = new Employee()
                    {
                        Id = "Employee/1",
                        Skills = new[] { new Skill { Name = "Coding", Level = 1 } },
                        Revision = DateTime.UtcNow
                    };
                    session.Store(employee);
                    session.SaveChanges();

                    var loadedEmployeeWithoutFirstOrDefault = session.Query<Employee>()
                        .Where(i => i.Id.StartsWith("Employee/1"))
                        .Select(e => new
                        {
                            e.Id,
                            e.Revision
                        })
                        .First();

                    var queryable = session.Query<Employee>()
                        .Where(i => i.Id.StartsWith("Employee/1"))
                        .Select(e => new
                        {
                            e.Id,
                            CodingSkillLevel = e.Skills.Where(s => s.Name == "Coding").FirstOrDefault().Level,
                            Revision = e.Revision
                        });

                    var loadedEmployeeWithFirstOrDefault = queryable
                        .First();

                    Assert.Equal(employee.Revision.Ticks, loadedEmployeeWithoutFirstOrDefault.Revision.Ticks);
                    Assert.Equal(employee.Revision.Ticks, loadedEmployeeWithFirstOrDefault.Revision.Ticks);
                }
            }
        }

    }
}
