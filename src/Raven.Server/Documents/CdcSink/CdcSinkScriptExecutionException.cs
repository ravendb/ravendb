using System;

namespace Raven.Server.Documents.CdcSink
{
    /// <summary>
    /// Signals that a CDC Sink patch (JS transformation script) threw while processing a document, so the
    /// per-document handler records it as a <see cref="ETL.TaskErrorStep.Transformation"/> error rather
    /// than a <see cref="ETL.TaskErrorStep.Load"/> one.
    /// </summary>
    internal sealed class CdcSinkScriptExecutionException : Exception
    {
        public CdcSinkScriptExecutionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
