using System;

namespace Raven.Server.Documents.CdcSink
{
    /// <summary>
    /// Signals that one or more documents in a CDC Sink batch could not be applied. Unlike a permanent
    /// <see cref="CdcSinkFaultedException"/>, this is transient: the retry loop enters fallback and retries
    /// from the last durable checkpoint. It is thrown deliberately to fail (and roll back) a batch that had
    /// per-document errors, so the checkpoint is never advanced past a row we could not apply - which on a
    /// CDC source (e.g. a PostgreSQL replication slot ack, which is monotonic) would drop that row for good.
    /// </summary>
    internal sealed class CdcSinkBatchFailedException : Exception
    {
        public CdcSinkBatchFailedException(string message)
            : base(message)
        {
        }
    }
}
