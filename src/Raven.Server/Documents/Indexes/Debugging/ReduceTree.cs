using System;

namespace Raven.Server.Documents.Indexes.Debugging
{
    public sealed class ReduceTree : IDisposable
    {
        public ReduceTreePage Root;

        public string Name;

        public string DisplayName;

        public int Depth { get; set; }

        public long PageCount { get; set; }

        public long NumberOfEntries { get; set; }

        public void Dispose()
        {
            Root?.Dispose();
        }
    }
}