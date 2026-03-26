using System;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25207 : StorageTest
{
    public RavenDB_25207(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CanDisableUpdatingLastWorkTimeInWriteTransaction()
    {
        var previousLastWorkTime = Env.LastWorkTime;
        using (var wTx = Env.WriteTransaction())
        {
            var tree = wTx.CreateTree(nameof(CanDisableUpdatingLastWorkTimeInWriteTransaction));
            tree.MultiAdd("test", "test");
            wTx.Commit();
        }
        
        Assert.NotEqual(previousLastWorkTime, Env.LastWorkTime);
        Assert.True(previousLastWorkTime < Env.LastWorkTime);
        previousLastWorkTime = Env.LastWorkTime;
        using (var wTx = Env.WriteTransaction())
        {
            wTx.LowLevelTransaction.DisableLastWorkTimeUpdate();
            var tree = wTx.CreateTree(nameof(CanDisableUpdatingLastWorkTimeInWriteTransaction));
            tree.MultiAdd("test", "test2");
            wTx.Commit();
        }
        
        Assert.Equal(previousLastWorkTime, Env.LastWorkTime);
    }
}
