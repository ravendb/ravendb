using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;

namespace Raven.Quill.Cdc;

internal static class CdcPerformanceReader
{
    public static async Task<CdcSinkPerformanceRaw> ReadAsync(
        MaintenanceOperationExecutor maintenance, CancellationToken ct)
    {
        try
        {
            return await maintenance.SendAsync(new GetCdcSinkPerformanceStatisticsOperation(), ct);
        }
        // telemetry must never 500: degrade to an empty snapshot
        catch (Exception e) when (e is not OperationCanceledException)
        {
            return new CdcSinkPerformanceRaw();
        }
    }

    /// <summary>Reads the persistent per-task error store, which holds the failures that never
    /// produced a batch; degrades to an empty result on any unavailability.</summary>
    public static async Task<CdcSinkErrorsRaw> ReadErrorsAsync(
        MaintenanceOperationExecutor maintenance, CancellationToken ct)
    {
        try
        {
            return await maintenance.SendAsync(new GetCdcSinkErrorsOperation(), ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            return new CdcSinkErrorsRaw();
        }
    }
}
