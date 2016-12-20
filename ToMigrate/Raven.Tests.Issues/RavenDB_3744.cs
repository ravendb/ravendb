// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1466.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3744
    {
        public class Employee
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public decimal Salary { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public void CanReturnDescriptiveParsingErrors()
        {
            var invalidOperatorEx = Assert.Throws<InvalidOperationException>(() => Facet<Employee>.Parse(x => x.Salary == 5));
            Assert.Equal("Cannot use Equal as facet range. Allowed operators: <, <=, >, >=.", invalidOperatorEx.Message);

            var invalidChaningEx = Assert.Throws<InvalidOperationException>(() => Facet<Employee>.Parse(x => x.Salary < 5 || x.Salary > 8));
            Assert.Equal("Range can be only specified using: \"&&\". Cannot use: \"OrElse\"", invalidChaningEx.Message);

            var rangeFieldsEx = Assert.Throws<InvalidOperationException>(() => Facet<Employee>.Parse(x => x.Salary < 5 && x.Salary > 8 && x.Salary > 60));
            Assert.Equal("Expressions on both sides of \"&&\" must point to range field. Ex. x => x.Age > 18 && x.Age < 99", rangeFieldsEx.Message);

            var differentFieldsEx = Assert.Throws<InvalidOperationException>(() => Facet<Employee>.Parse(x => x.Salary < 5 && x.Age > 15 ));
            Assert.Equal("Different range fields were detected: \"Salary\" and \"Age\"", differentFieldsEx.Message);

            var invalidOperatorsInChainEx = Assert.Throws<InvalidOperationException>(() => Facet<Employee>.Parse(x => x.Salary == 5 && x.Salary == 9));
            Assert.Equal("Members in sub-expression(s) are not the correct types (expected \"<\", \"<=\", \">\" or \">=\")", invalidOperatorsInChainEx.Message);

            var invalidRange1Ex = Assert.Throws<InvalidOperationException>(() => Facet<Employee>.Parse(x => x.Salary < 5 && x.Salary >= 15));
            Assert.Equal("Invalid range: 15..5", invalidRange1Ex.Message);

            var parsedRange1 = Facet<Employee>.Parse(x => x.Salary >= 5 && x.Salary <= 15);
            Assert.Equal("{Dx5 TO Dx15}", parsedRange1);

            var invalidRange2Ex = Assert.Throws<InvalidOperationException>(() => Facet<Employee>.Parse(x => x.Salary >= 15 && x.Salary <= 5));
            Assert.Equal("Invalid range: 15..5", invalidRange2Ex.Message);

            var parsedRange2 = Facet<Employee>.Parse(x => x.Salary <= 15 && x.Salary >= 5);
            Assert.Equal("{Dx5 TO Dx15}", parsedRange2);

            var parsedRange3 = Facet<Employee>.Parse(x => x.Salary >= 5 && x.Salary <= 5);
            Assert.Equal("{Dx5 TO Dx5}", parsedRange3);
        }

    }
}
