using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents.Indexes.MapReduce.Exceptions;
using Raven.Server.Exceptions;
using Sparrow.Server.Exceptions;

namespace Raven.Server.Documents.Indexes
{
    public class IndexingRunStats
    {
        public int MapAttempts;
        public int MapReferenceAttempts;
        public int MapSuccesses;
        public int MapReferenceSuccesses;
        public int MapErrors;
        public int MapReferenceErrors;

        public int ReduceAttempts;
        public int ReduceSuccesses;
        public int ReduceErrors;

        public int IndexingOutputs;
        public Size AllocatedBytes;
        public long DocumentsSize;

        public int? EntriesCount;

        public ReduceRunDetails ReduceDetails;

        public MapRunDetails MapDetails;

        public StorageCommitDetails CommitDetails;

        public List<IndexingError> Errors;

        public int MaxNumberOfOutputsPerDocument;

        public int NumberOfKeptReduceErrors;

        public override string ToString()
        {
            return $"Map - attempts: {MapAttempts}, successes: {MapSuccesses}, errors: {MapErrors} / " +
                   $"Reduce - attempts: {ReduceAttempts}, successes: {ReduceSuccesses}, errors: {ReduceErrors}";
        }

        public void AddMapError(string key, string message)
        {
            AddError(key, message, "Map");
        }

        public void AddMapReferenceError(string key, string message)
        {
            AddError(key, message, "MapReference");
        }

        public void AddReduceError(string message)
        {
            AddError(null, message, "Reduce");

            NumberOfKeptReduceErrors++;
        }

        public void AddCorruptionError(Exception exception)
        {
            AddError(null, $"Index corruption occurred: {exception}", "Corruption");
        }

        public void AddWriteError(IndexWriteException exception)
        {
            AddError(null, $"Write exception occurred: {exception}", "Write");
        }

        public void AddAnalyzerError(IndexAnalyzerException exception)
        {
            AddError(null, $"Could not create analyzer: {exception}", "Analyzer");
        }

        public void AddUnexpectedError(Exception exception)
        {
            AddError(null, $"Unexpected exception occurred: {exception}", "Critical");
        }

        public void AddCriticalError(Exception exception)
        {
            AddError(null, $"Critical exception occurred: {exception}", "Critical");
        }

        public void AddMemoryError(Exception oome)
        {
            AddError(null, $"Memory exception occurred: {oome}", "Memory");
        }

        public void AddExcessiveNumberOfReduceErrors(ExcessiveNumberOfReduceErrorsException e)
        {
            AddError(null, $"Excessive number of reduce errors: {e}", "Reduce");
        }

        public void AddDiskFullError(DiskFullException e)
        {
            AddError(null, $"Disk full error occured: {e}", "Disk Full");
        }

        private void AddError(string key, string message, string action)
        {
            if (Errors == null)
                Errors = new List<IndexingError>();

            Errors.Add(new IndexingError
            {
                Action = action ?? string.Empty,
                Document = key ?? string.Empty,
                Timestamp = SystemTime.UtcNow,
                Error = message ?? string.Empty
            });
        }
    }
}
