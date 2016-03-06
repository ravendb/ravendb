using System;

namespace Raven.Server.Documents.Indexes.Persistance
{
    public interface IIndexWriteActions : IDisposable
    {
        void IndexDocument(Document document);

        void Delete(string key);
    }
}