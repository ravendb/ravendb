using System.Collections.Frozen;
using System.Collections.Generic;
using Voron.Data.BTrees;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;

namespace Voron;

public record EnvironmentStateRecord(
    Pager.State DataPagerState,
    long TransactionId,
    long FlushedToJournal,
    ScratchPagesSnapshot ScratchPagesTable,
    TreeRootHeader Root,
    long NextPageNumber,
    // This represent the *current* journal state, which may involve
    // writes from _other_ envs due to shared journals
    (long Number, long Last4KWritePosition) Journal,
    List<(long Start, long Count)> SparseRegions,
    IReadOnlyCollection<PageFromScratchBuffer> PagesAllocatedInTransaction,
    object ClientState);
