using System.Collections.Generic;
using Voron.Impl.Scratch;

namespace Voron.Impl;

record ApplyLogsToDataFileState(
    List<PageFromScratchBuffer> Buffers,
    EnvironmentStateRecord Record)
{
    public override string ToString()
    {
        return Record.DataPagerState.Pager.FileName;
    }
}
