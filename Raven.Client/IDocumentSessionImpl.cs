using System;

namespace Raven.Client
{
    public interface IDocumentSessionImpl : IDocumentSession
    {
        void Commit(Guid txId);
        void Rollback(Guid txId);
    }
}