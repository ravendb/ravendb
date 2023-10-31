using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;
using Random = System.Random;

namespace SlowTests.Issues;

public class RavenDB_21399_2 : StorageTest
{
    public RavenDB_21399_2(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Corax)]
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

        var rootState = new Lookup<Int64LookupKey>.CursorState {Page = wtx.LowLevelTransaction.GetPage(lookup.State.RootPage)};

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
        lookup.Add(keyData - 2, 0);

        // Problem
        lookup.VerifyStructure();
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Corax)]
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


        lookup.Render();
        // here we wait to add *another* page to the right most branch, so we'll have:
        // * Root
        // * * Branch - min value
        // * * Branch - $SomeVal
        // * * Branch - $SomeVal + 100

        var rootState = new Lookup<Int64LookupKey>.CursorState {Page = wtx.LowLevelTransaction.GetPage(lookup.State.RootPage)};

        long keyData = Lookup<Int64LookupKey>.GetKeyData(ref rootState, rootState.Header->NumberOfEntries / 2);

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

        lookup.Add(keyData - 2, 0);

        // Problem
        lookup.VerifyStructure();
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Corax)]
    public void CanSafelySwapLeavesWithoutRemovingTermFromDisk()
    {
        using var wtx = Env.WriteTransaction();
        CompactTree tree = wtx.CompactTreeFor($"test");
        var lookup = tree._inner;
        long k = 1;

        while (lookup.State.BranchPages < 4)
        {
            tree.Add($"{k}", k++);
        }

        while (k > 0)
        {
            tree.TryRemove($"{--k}", out _);
        }
    }

   [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Corax)]
   [InlineData(1274580600)]
    public void RemoveBranchAndMoveAllItemsIntoAnother(int seed)
    {
        using var wtx = Env.WriteTransaction();
        CompactTree tree = wtx.CompactTreeFor($"test");
        var lookup = tree._inner;
        long k = 0;
        while (lookup.State.BranchPages < 4)
        {
            tree.Add($"{k}", k++);
        }

        var list = Enumerable.Range(0, (int)k).ToList();
        var random = new Random(seed);
        
        while (list.Count > 0)
        {
            var index = random.Next(0, list.Count);
            var elementToRemove = list[index];
            Assert.True(tree.TryRemove($"{elementToRemove}", out _));
            list.RemoveAt(index);
        }
    }
    
    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Corax)]
    [InlineData(228422771, 2000, 5_834_460)]
    public void TermWillNotHitMaximumReferenceCount(int seed, int maximumElements, long operationCount)
    {
        Transaction wtx = null;
        try
        {
            wtx = Env.WriteTransaction();
            CompactTree tree = wtx.CompactTreeFor($"test");
            var lookup = tree._inner;
            var buffer = new long[maximumElements];
            List<int> currentIndex = new();
            List<int> freeElements = Enumerable.Range(0, maximumElements).Select(x => x).ToList();
            var random = new Random(seed);
            var counter = operationCount;
            while (counter >= 0)
            {
                counter--;

                var action = (Action)(Math.Abs(random.Next()) % 2);
                if (action is Action.Add && freeElements.Count > 0)
                {
                    var indexOfElementToUpdate = random.Next(0, freeElements.Count);
                    var elementToInsert = freeElements[indexOfElementToUpdate];
                    freeElements.RemoveAt(indexOfElementToUpdate);
                    tree.Add($"{elementToInsert}", elementToInsert);
                    currentIndex.Add(elementToInsert);
                }
                else if (action is Action.Remove && currentIndex.Count > 0)
                {
                    var indexOfElementToRemove = random.Next(0, currentIndex.Count);
                    var elementToRemove = currentIndex[indexOfElementToRemove];
                    currentIndex.RemoveAt(indexOfElementToRemove);
                    freeElements.Add(elementToRemove);
                    Assert.True(tree.TryRemove($"{elementToRemove}", out var value));
                }

                if (lookup.CheckTreeStructureChanges().Changed)
                {
                    wtx.Commit();
                    wtx = Env.WriteTransaction();
                    tree = wtx.CompactTreeFor($"test");
                    lookup = tree._inner;
                }
            }
        }
        finally
        {
            wtx?.Dispose();
        }
    }

    private enum Action : int
    {
        Add = 0,
        Remove = 1
    }
}
