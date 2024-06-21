using System.Collections.Frozen;
using System.Collections.Generic;
using Voron.Impl.Paging;

namespace Voron;

public record EnvironmentStateRecord(
    Pager2.State DataPagerState, 
    long TransactionId,
    FrozenSet<Pager2.State> StatesStrongRefs, //This is here to ensure the GC won't clean-up the states behind our backs
    FrozenDictionary<long, Page> ScratchPagesTable
    );

