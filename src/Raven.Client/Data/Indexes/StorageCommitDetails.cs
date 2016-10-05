using System;
using Sparrow.Json;

namespace Raven.Client.Data.Indexes
{
    public class StorageCommitDetails : IndexingPerformanceOperation.IDetails
    {
        public int NumberOfModifiedPages { get; set; }

        public int NumberOfPagesWrittenToDisk { get; set; }

        public void ToJson(BlittableJsonTextWriter writer, JsonOperationContext context)
        {
            throw new NotImplementedException(); // TODO remove after merging Pawel's changes
        }
    }
}
