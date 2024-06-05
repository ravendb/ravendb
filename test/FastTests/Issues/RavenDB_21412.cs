using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.OngoingTasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_21412 : RavenTestBase
{
    public RavenDB_21412(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public void Validate_All_OngoingTaskTypes_Are_Added_To_Snmp()
    {
        var enumsAddedToSnmp = new HashSet<OngoingTaskType> { };

        foreach (var enumValue in Enum.GetValues<OngoingTaskType>())
        {
            Assert.True(enumsAddedToSnmp.Contains(enumValue), $"enumsAddedToSnmp.Contains({enumValue}) => please add '{enumValue}' as a part of 5.1.11 OID family and update the '{nameof(enumsAddedToSnmp)}' hashset.");
        }
    }
}
