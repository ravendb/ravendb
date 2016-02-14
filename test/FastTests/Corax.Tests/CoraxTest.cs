using System;
using Raven.Server.Indexing.Corax;
using Raven.Server.Indexing.Corax.Analyzers;
using Voron;

namespace Tryouts.Corax.Tests
{
    public class CoraxTest : IDisposable
    {
        protected readonly FullTextIndex _fullTextIndex;

        protected virtual IAnalyzer CreateAnalyzer()
        {
            return new DefaultAnalyzer();
        }

        public CoraxTest()
        {
            _fullTextIndex = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), CreateAnalyzer());
        }

        public void Dispose()
        {
            _fullTextIndex?.Dispose();
        }
    }
}