using System;

namespace Raven.Server.Documents.Indexes.Persistance
{
    public interface IIndexWriteActions : IDisposable
    {
        void Write(Document document);

        void Delete(string key);
    }
}