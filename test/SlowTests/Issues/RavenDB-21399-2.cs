using FastTests.Voron;
using Voron.Data.Lookups;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21399_2 : StorageTest
{
    public RavenDB_21399_2(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public unsafe void CanHandleDeletesAndUpdatesToLeftmostLeafPage()
    {
        using var wtx = Env.WriteTransaction();

        var lookup = wtx.LookupFor<Int64LookupKey>("test");
        long k = 1;
        
        // we are waiting until the tree structure looks like
        // * Root
        // * * Branch - min value
        // * * Branch - SomeVal

        while (lookup.State.BranchPages < 2)
        {
            lookup.Add(k++, 0);
        }

        // here we wait to add *another* page to the right most branch, so we'll have:
        // * Root
        // * * Branch - min value
        // * * Branch - $SomeVal
        // * * * Leaf -  $SomeVal
        // * * * Leaf -  $SomeVal + 10
        // * * * Leaf -  $SomeVal + 20
        var changed = lookup.CheckTreeStructureChanges();
        while (changed.Changed == false)
        {
            lookup.Add(k++, 0);
        }
        
        var rootState = new Lookup<Int64LookupKey>.CursorState
        {
            Page = wtx.LowLevelTransaction.GetPage(lookup.State.RootPage)
        };

        long keyData = Lookup<Int64LookupKey>.GetKeyData(ref rootState, rootState.Header->NumberOfEntries - 1);
        changed = lookup.CheckTreeStructureChanges();
        
        // Now we remove until we have this situation:
        // * Root
        // * * Branch - min value
        // * * Branch - $SomeVal
        //  REMOVED THIS ONE: --- * * * Leaf -  $SomeVal
        // * * * Leaf -  $SomeVal + 10
        // * * * Leaf -  $SomeVal + 20
        while (changed.Changed == false)
        {
            lookup.TryRemove(keyData++, out _);
        }
        
        // Now we insert $SomeVal+8, which will go to the $SomeVal + 10 key 
        lookup.Add(keyData-2, 0);
        
        // Problem
        lookup.VerifyStructure();
    }
    
    [Fact]
    public unsafe void CanHandleDeletesAndUpdatesToMiddleLeafPage()
    {
        using var wtx = Env.WriteTransaction();

        var lookup = wtx.LookupFor<Int64LookupKey>("test");
        long k = 1;
        
        // we are waiting until the tree structure looks like
        // * Root
        // * * Branch - min value
        // * * Branch - SomeVal
        // * * Branch - SomeVal + 100

        while (lookup.State.BranchPages < 4)
        {
            lookup.Add(k++, 0);
        }

        // here we wait to add *another* page to the right most branch, so we'll have:
        // * Root
        // * * Branch - min value
        // * * Branch - $SomeVal
        // * * Branch - $SomeVal + 100

        var rootState = new Lookup<Int64LookupKey>.CursorState
        {
            Page = wtx.LowLevelTransaction.GetPage(lookup.State.RootPage)
        };

        long keyData = Lookup<Int64LookupKey>.GetKeyData(ref rootState, rootState.Header->NumberOfEntries /2);
        
        // Now we remove until we have this situation:
        // * Root
        // * * Branch - min value
        // REMOVED THIS ONE: ---- * * Branch - $SomeVal
        // * * Branch - $SomeVal + 100

        long branches = lookup.State.BranchPages;
        while (branches == lookup.State.BranchPages)
        {
            lookup.TryRemove(keyData++, out _);
        }

        lookup.Add(keyData-2, 0);
        
        // Problem
        lookup.VerifyStructure();
    }
}
