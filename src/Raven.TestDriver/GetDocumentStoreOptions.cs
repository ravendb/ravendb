using System;

namespace Raven.TestDriver
{
    public class GetDocumentStoreOptions
    {
        internal static GetDocumentStoreOptions Default = new GetDocumentStoreOptions();

        public TimeSpan? WaitForIndexingTimeout { get; set; }
    }
}
