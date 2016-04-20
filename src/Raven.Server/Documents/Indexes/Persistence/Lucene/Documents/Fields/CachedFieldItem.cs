using System;

using Lucene.Net.Documents;
using Raven.Server.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields
{
    public class CachedFieldItem<T> : IDisposable where T : AbstractField
    {
        public T Field;
        private LazyStringReader _reader;

        public LazyStringReader LazyStringReader
        {
            get { return _reader ?? (_reader = new LazyStringReader()); }
            set { _reader = value; }
        }

        public void Dispose()
        {
            _reader?.Dispose();
        }
    }
}