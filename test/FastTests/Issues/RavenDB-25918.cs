using System;
using System.Linq;
using Raven.Server.NotificationCenter.Notifications;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues;

public class RavenDB_25918 : RavenTestBase
{
    public RavenDB_25918(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Codebase)]
    public void AssertNotificationReasonsIntegrity()
    {
        // We want to make sure new enum values of AlertReason and PerformanceHintReason are added after existing ones.
        // After RavenDB-24424 changes we store these values in storage in form of integers, not strings.
        // Adding new values in between existing values will cause existing notifications to be read with incorrect reason.
        //
        // Make sure new value fulfills the above and update the assertion.
        Assert.Equal(74, Enum.GetNames(typeof(AlertReason)).Length);
        Assert.Equal(AlertReason.HighReadAheadKb, Enum.GetValues(typeof(AlertReason)).Cast<AlertReason>().Last());
        
        Assert.Equal(10, Enum.GetNames(typeof(PerformanceHintReason)).Length);
        Assert.Equal(PerformanceHintReason.Indexing_References, Enum.GetValues(typeof(PerformanceHintReason)).Cast<PerformanceHintReason>().Last());
    }
}
