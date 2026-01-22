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
    long FlushedToJournal,
    ImmutableDictionary<long, PageFromScratchBuffer> ScratchPagesTable,
    TreeRootHeader Root,
    long NextPageNumber,
    // This represent the *current* journal state, which may involve
    // writes from _other_ envs due to shared journals
    (long Number, long Last4KWritePosition) Journal,
    List<(long Start, long Count)> SparseRegions,
    object ClientState);

internal sealed class EnvironmentStateRecordHolder
{
    public EnvironmentStateRecord EnvStateRecord;
}
