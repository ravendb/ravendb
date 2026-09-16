using System;

namespace Raven.Server.Documents.CdcSink
{
    /// <summary>
    /// Signals a permanent CDC Sink configuration/schema error that retrying cannot fix - e.g. a
    /// configured table that does not resolve in the process's table mapping. The retry loop
    /// (<see cref="CdcSinkProcess.RunWithRetryAsync"/>) treats this differently from a transient
    /// failure: instead of entering fallback and retrying forever, it moves the process to a faulted
    /// state and stops. Correcting the configuration recreates the process
    /// (<c>CdcSinkLoader.HandleDatabaseRecordChange</c>), which clears the fault and starts fresh.
    /// </summary>
    internal sealed class CdcSinkFaultedException : Exception
    {
        public CdcSinkFaultedException(string message)
            : base(message)
        {
        }

        public CdcSinkFaultedException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
