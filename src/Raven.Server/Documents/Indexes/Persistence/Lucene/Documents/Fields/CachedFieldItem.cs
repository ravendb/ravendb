using System;
using System.IO;
using System.Text;
using Lucene.Net.Documents;
using Raven.Server.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields
{
    public class CachedFieldItem<T> : IDisposable where T : AbstractField
    {
        public T Field;

        public LazyStringReader LazyStringReader { get; set; }

        public BlittableObjectReader BlittableObjectReader { get; set; }

        public void Dispose()
        {
            LazyStringReader?.Dispose();
            BlittableObjectReader?.Dispose();
        }
    }
}