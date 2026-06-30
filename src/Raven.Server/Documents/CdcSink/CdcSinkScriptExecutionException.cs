using System;

namespace Raven.Server.Documents.CdcSink
{
    /// <summary>
    /// Signals that a CDC Sink patch (JS transformation script) threw while processing a document.
    /// Thrown by the batch command's RunPatches and caught by the per-document handler in
    /// <c>CdcSinkBatchCommand.ExecuteCmd</c>, which records it once as a
    /// <see cref="ETL.TaskErrorStep.Transformation"/> error (other tolerable per-document failures are
    /// recorded as <see cref="ETL.TaskErrorStep.Load"/>).
    /// </summary>
    internal sealed class CdcSinkScriptExecutionException : Exception
    {
        public CdcSinkScriptExecutionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
