using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class LoadAsyncValueTypeWithIncludes : RavenTestBase
    {
        [Fact]
        public async Task TestLoadAsyncWithValueType()
        {
            using (var documentStore = NewDocumentStore())
            {
                var user1 = new User { Name = "Fitzchak Yitzchaki" };
                var user2 = new User { Name = "Oren Eini" };
                var user3 = new User { Name = "Maxim Buryak" };
                var user4 = new User { Name = "Grisha Kotler" };
                var user5 = new User { Name = "Michael Yarichuk" };
                var companyId = string.Empty;
                var companyShortId = 0;

                using (var asyncSession = documentStore.OpenAsyncSession())
                {
                    #region Setting company
                    await asyncSession.StoreAsync(user1);
                    await asyncSession.StoreAsync(user2);
                    await asyncSession.StoreAsync(user3);
                    await asyncSession.StoreAsync(user4);
                    await asyncSession.StoreAsync(user5);

                    await asyncSession.SaveChangesAsync();


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

                    #endregion
                    var company = new Company
                    {
                        Id = "Companies/1",
                        Name = "Relaymark",
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

                    await asyncSession.StoreAsync(company);
                    await asyncSession.SaveChangesAsync();
                     
                    var idx = company.Id.LastIndexOf('/');
                    companyShortId = int.Parse(company.Id.Substring(idx + 1));
                }
                using (var session = documentStore.OpenSession())
                {
                    var returnedCompanySync = session.Include<Employee, User>(x => x.UserId)
                        .Load<Company>(companyShortId);

                    Assert.NotNull(returnedCompanySync);
                }
                using (var asyncSession = documentStore.OpenAsyncSession())
                {
                    var returnedCompanyWithoutIncludes = await asyncSession.LoadAsync<Company>(companyShortId);
                    var returnedCompany = await asyncSession.Include<Employee, User>(x => x.UserId)
                        .LoadAsync<Company>(companyShortId); //fails  

                    Assert.NotNull(returnedCompanyWithoutIncludes);
                    Assert.NotNull(returnedCompany);
                }
            }
        }

        public class Company 
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public BusinessUnit BusinessUnit { get; set; }
        }
        public class BusinessUnit
        {
            public List<BusinessUnit> BusinessUnits { get; set; }
            public List<Employee> Employees { get; set; }
            public string Name { get; set; }
        }

        public class Employee
        {
            public string Position { get; set; }
            public string UserId { get; set; } 
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        } 
    }
}

