using System;
using System.IO.Compression;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.BulkInsert
{
    public class BulkInsertOptions
    {
        public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.NoCompression;

        /// <summary>
        /// Determines whether we should skip overwriting a document when it is updated by exactly the same document (by comparing the content and the metadata)
        /// </summary>
        public bool SkipOverwriteIfUnchanged { get; set; }
    }
}
