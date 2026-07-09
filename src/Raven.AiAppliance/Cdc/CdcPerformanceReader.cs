using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;

namespace Raven.AiAppliance.Cdc;

/// <summary>
/// Single home for reading RavenDB's rolling CDC-sink perf window. Telemetry must never
/// 500 its callers, so an unavailable feed (feature off, older server, parse hiccup)
/// degrades to an empty snapshot; cancellation still propagates. Shared by the CDC page
/// endpoint and the App Usage read path.
/// </summary>
internal static class CdcPerformanceReader
{
    public static async Task<CdcSinkPerformanceRaw> ReadAsync(
        MaintenanceOperationExecutor maintenance, CancellationToken ct)
    {
        try
        {
            return await maintenance.SendAsync(new GetCdcSinkPerformanceStatisticsOperation(), ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            return new CdcSinkPerformanceRaw();
        }
    }
}
