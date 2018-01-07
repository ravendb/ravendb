using System;

namespace Raven.TestDriver
{
    public class GetDocumentStoreOptions
    {
        internal static GetDocumentStoreOptions Default => _default.Value;
        private static readonly Lazy<GetDocumentStoreOptions> _default = new Lazy<GetDocumentStoreOptions>(() => new GetDocumentStoreOptions());
        public TimeSpan? WaitForIndexingTimeout { get; set; }
    }
}
