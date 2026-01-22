using System;
using System.Collections.Generic;
using Voron.Impl.Scratch;

namespace Voron.Impl;

record ApplyLogsToDataFileState(
    List<PageFromScratchBuffer> Buffers,
    List<(long Start, long Count)> SparseRegions,
    EnvironmentStateRecord Record)
{
    public override string ToString()
    {
        return Record.DataPagerState.Pager.FileName;
    }
}
