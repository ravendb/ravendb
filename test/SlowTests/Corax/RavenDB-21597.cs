using System;
using FastTests;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_21597 : NoDisposalNeeded
{
    public RavenDB_21597(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public void CoraxTestsAreRunningOnCorrectVersionOnly()
    {
        if (Raven.Server.Documents.Indexes.IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion >= 60_000)
#pragma warning disable CS0162 // Unreachable code detected
            throw new InvalidOperationException($"The commit from 'RavenDB-21597' needs to be reverted since v6.0 in order to run all of Corax's tests properly.");
#pragma warning restore CS0162 // Unreachable code detected
    }
}
