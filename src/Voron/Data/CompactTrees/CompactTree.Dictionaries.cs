using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;

namespace Voron.Data.CompactTrees;


// The heuristics to rebuild the compression dictionaries can be tricky. 
// There are many ways to regenerate dictionaries to obtain better compression, but they are always a gamble.
// The most important decision to make is either to use global scopes or to use local scopes.
// Global scopes may be the best decision, because they are the most likely to be successful in the long run.
// However, they are also the most expensive to build.
// Local scopes are less likely to succeed, but they are cheaper to build. The way to ensure high levels of
// compression is to create as many dictionaries specialized for each page. This is prohibitively expensive,
// and the tradeoff is that it is not possible to use a unique dictionary for each page. Finding the right
// heuristic for each clusters of keys is a very difficult problem. Therefore, when using local scopes we
// should be careful where the boundaries of each dictionary is used; which in return would increase the runtime
// work needed to be done for the housekeeping.
// 
// For local scopes we could heuristically create dictionaries during splitting based on the data that we encounter.
//  - Pro: the dictionary will have a tighter compression ratio,
//  - Pro: we could avoid splitting pages just upgrading the dictionary,
//  - Cons: many more dictionaries to track, higher performance burden on page splits.
// 
// For global scope dictionaries we could perform offline sampling of the whole database as a maintenance tasks
// after ceratin conditions are met (like doubling the amount of keys) but build a single dictionary that can
// compress the whole tree in the most efficient way (if needed).
//  - Pro: There is only a single active dictionary that everybody uses,
//  - Pro: The performance burden could be paid as an idle task and happen at the moment of splitting,
//  - Pro: The pages could be upgraded over time when performing writes on a particular page to amortize the IO cost,
//  - Pro: Random selection or fixed budget would do the trick of adaptively upgrading the tree compression over time,
//  - Cons: Requires to have a task to perform the dictionary creation outside of the insertion process,
//  - Cons: Compression ratio will not be THAT tight per single page,
//  - Cons: We may need larger dictionaries to accomodate better compression ratios. 
//
//  In Voron we use global scopes dictionaries because they have a cleaner implementation with less intrusive processes
//  where it really matters (writing). 

unsafe partial class CompactTree
{
    public bool TryImproveDictionary<TKeyScanner, TKeyVerifier>( TKeyScanner trainer, TKeyVerifier tester )
        where TKeyScanner : struct, IReadOnlySpanEnumerator
        where TKeyVerifier : struct, IReadOnlySpanEnumerator
    {
        Debug.Assert(_llt.Flags == TransactionFlags.ReadWrite);

        // We will try to improve the dictionary by scanning the whole tree over a maximum budget.    
        // We will do this by randomly selecting a page and then randomly selecting a key from that page.
        var currentDictionary = GetEncodingDictionary(this._llt, _state.TreeDictionaryId);
        var newDictionary = PersistentDictionary.CreateIfBetter(_llt, trainer, tester, currentDictionary);

        // If the new dictionary is actually better, then update the current dictionary at the tree level.    
        if (currentDictionary != newDictionary)
            _state.TreeDictionaryId = newDictionary.PageNumber;

        // We will update the number of entries regardless if we updated the current dictionary or not. 
        _state.NextTrainAt = (long)(Math.Max(_state.NextTrainAt, _state.NumberOfEntries) * 1.5);
        return true;
    }


    /// <summary>
    /// We will try to improve the dictionary by scanning the whole tree and using the data. This may be costly and alternative and probably
    /// less effective solutions may need to be developed for when this method becomes prohibitive. 
    /// </summary>    
    public bool TryImproveDictionaryByFullScanning()
    {
        return TryImproveDictionary(new SequentialKeyScanner(this), new SequentialKeyScanner(this));
    }

    /// <summary>
    /// We will try to improve the dictionary by sampling from the tree. This may be less costly but at the same time
    /// it could be less effective. 
    /// </summary>
    public bool TryImproveDictionaryByRandomlyScanning(long samples, int? seed = null)
    {
        return TryImproveDictionary(
            new RandomBranchKeyScanner(this, samples, seed), 
            new RandomBranchKeyScanner(this, samples, seed));
    }
}
