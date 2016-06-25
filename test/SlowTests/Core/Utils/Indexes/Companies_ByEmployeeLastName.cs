using System.Linq;

using Raven.Client.Indexes;
using SlowTests.Core.Utils.Entities;

namespace SlowTests.Core.Utils.Indexes
{
    public class Companies_ByEmployeeLastName : AbstractIndexCreationTask<Company>
    {
        public class Result
        {
            public string LastName { get; set; }
        }

        public Companies_ByEmployeeLastName()
        {
            Map = companies => from company in companies
                               from employee in LoadDocument<Employee>(company.EmployeesIds)
                               select new
                               {
                                   LastName = employee.LastName
                               };
        }
    }
}