using System;
using Sparrow.Json;

namespace Raven.NewClient.Client.Data.Indexes
{
    public class StorageCommitDetails 
    {
        public int NumberOfModifiedPages { get; set; }

        public int NumberOfPagesWrittenToDisk { get; set; }

    }
}
