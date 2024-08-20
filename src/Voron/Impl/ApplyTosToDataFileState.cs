using System.Collections.Generic;
using Voron.Impl.Scratch;

namespace Voron.Impl;

record ApplyLogsToDataFileState(
    List<PageFromScratchBuffer> Buffers,
    List<long> SparseRegions,
    EnvironmentStateRecord Record);
