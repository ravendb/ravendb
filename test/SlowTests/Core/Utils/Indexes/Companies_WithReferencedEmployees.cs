// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Utils.Indexes
{
    public class Companies_WithReferencedEmployees : AbstractIndexCreationTask<Company>
    {
        public class CompanyEmployees
        {
            public string Name { get; set; }
            public List<string> Employees { get; set; }
        }

        public Companies_WithReferencedEmployees()
        {
            Map = companies => from company in companies
                           select new
                           {
                               Name = company.Name,
                               EmployeesIds = company.EmployeesIds
                           };
        }
    }
}
