using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.Immutable;
using Voron.Data.BTrees;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;

namespace Voron;

public record EnvironmentStateRecord(
    Pager.State DataPagerState, 
    long TransactionId,
    ImmutableDictionary<long, PageFromScratchBuffer> ScratchPagesTable,
    long WrittenToJournalNumber,
    TreeRootHeader Root,
    long NextPageNumber,
    (JournalFile Current, long Last4KWritePosition) Journal,
    object ClientState,
    List<long> SparsePageRanges);
