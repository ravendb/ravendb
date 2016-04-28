// -----------------------------------------------------------------------
//  <copyright file="UsersWithCustomDataAndInclude.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

using Company = SlowTests.Core.Utils.Entities.Company;
using Employee = SlowTests.Core.Utils.Entities.Employee;

namespace SlowTests.Core.Utils.Transformers
{
    public class CompanyEmployeesTransformer : AbstractTransformerCreationTask<Company>
    {
        public CompanyEmployeesTransformer()
        {
            TransformResults = companies => from company in companies
                                                      select new
                                                      {
                                                          Name = company.Name,
                                                          Employees = company.EmployeesIds.Select(x => LoadDocument<Employee>(x).LastName)
                                                      };
        }
    }
}
