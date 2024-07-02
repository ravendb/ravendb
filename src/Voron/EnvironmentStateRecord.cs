using System.Collections.Frozen;
using Voron.Data.BTrees;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;

namespace Voron;

public record EnvironmentStateRecord(
    Pager2.State DataPagerState, 
    long TransactionId,
    FrozenDictionary<long, PageFromScratchBuffer> ScratchPagesTable,
    long FlushedToJournal,
    TreeMutableState Root,
    long NextPageNumber
    );
