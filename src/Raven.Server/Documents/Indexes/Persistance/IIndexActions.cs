using System;

namespace Raven.Server.Documents.Indexes.Persistance
{
    public interface IIndexActions : IDisposable
    {
        void Write(global::Lucene.Net.Documents.Document document);

        void Delete(string key);
    }
}