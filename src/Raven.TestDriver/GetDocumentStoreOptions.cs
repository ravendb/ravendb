using System;

namespace Raven.TestDriver
{
    public class GetDocumentStoreOptions
    {
        internal static GetDocumentStoreOptions Default => DefaultLazy.Value;
        private static readonly Lazy<GetDocumentStoreOptions> DefaultLazy = new Lazy<GetDocumentStoreOptions>(() => new GetDocumentStoreOptions());

        public TimeSpan? WaitForIndexingTimeout { get; set; }
    }
}
