using System;
using Xunit;

namespace RavenDbIssues
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Raven.Tests.Helpers;

    public class LoadAsyncWithHierarchicalIncludesTest : RavenTestBase
    {
        [Fact]
        public async Task SampleTestMethod()
        {
            using (var documentStore = NewDocumentStore())
            {
                var user1 = new User { Name = "Fitzchak Yitzchaki" };
                var user2 = new User { Name = "Oren Eini" };
                var user3 = new User { Name = "Maxim Buryak" };
                var user4 = new User { Name = "Grisha Kotler" };
                var user5 = new User { Name = "Michael Yarichuk" };
                var companyId = string.Empty;

                using (var asyncSession = documentStore.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(user1);
                    await asyncSession.StoreAsync(user2);
                    await asyncSession.StoreAsync(user3);
                    await asyncSession.StoreAsync(user4);
                    await asyncSession.StoreAsync(user5);

                    await asyncSession.SaveChangesAsync();

                    #region Setting company

                    var employeeList1 = new List<Employee>();
                    employeeList1.Add(new Employee
                    {
                        Position = "Financial Director",
                        UserId = user1.Id
                    });
                    employeeList1.Add(new Employee
                    {
                        Position = "Director",
                        UserId = user2.Id
                    });

                    var employeeList2 = new List<Employee>();
                    employeeList2.Add(new Employee
                    {
                        Position = "Lead Manager",
                        UserId = user3.Id
                    });
                    employeeList2.Add(new Employee
                    {
                        Position = "Lead Developer",
                        UserId = user4.Id
                    });

                    var company = new Company
                    {
                        BusinessUnit = new BusinessUnit
                        {
                            Name = "Main Business Unit",
                            Employees = employeeList1,
                            BusinessUnits = new List<BusinessUnit>
                            {
                                new BusinessUnit
                                {
                                    Name = "Paris Headquarters",
                                    Employees = employeeList2
                                }
                            }
                        }
                    };

                    #endregion

                    await asyncSession.StoreAsync(company);
                    await asyncSession.SaveChangesAsync();

                    companyId = company.Id;
                }

                using (var session = documentStore.OpenSession())
                {
                    var returnedCompanySync = session.Include<Employee, User>(x => x.UserId)
                                                     .Load<Company>(companyId);

                    Assert.NotNull(returnedCompanySync);
                }
                using (var asyncSession = documentStore.OpenAsyncSession())
                {
                    var returnedCompany = await asyncSession.Include<Employee, User>(x => x.UserId)
                                                            .LoadAsync<Company>(companyId); //fails 
                    Assert.NotNull(returnedCompany);
                }
            }
        }

        public class Company
        {
            public BusinessUnit BusinessUnit;
            public string Id;
            public string Name;
        }

        public class BusinessUnit
        {
            public List<BusinessUnit> BusinessUnits;
            public List<Employee> Employees;
            public string Name;
        }

        public class Employee
        {
            public string Position;
            public string UserId;
        }

        public class User
        {
            public string Id;
            public string Name;
        }
    }
}
