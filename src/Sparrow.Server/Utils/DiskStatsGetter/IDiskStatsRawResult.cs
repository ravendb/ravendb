using System;

namespace Sparrow.Server.Utils.DiskStatsGetter;

internal interface IDiskStatsRawResult
{
    DateTime Time { get; }
}
