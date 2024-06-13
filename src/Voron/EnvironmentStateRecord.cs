using System.Collections.Frozen;
using System.Collections.Generic;
using Voron.Impl.Paging;

namespace Voron;

public record EnvironmentStateRecord(Pager2.State DataPagerState);

// public List<Pager2.State> ScratchesState = [];
// public FrozenDictionary<long, Page> ScratchPagesTable;
