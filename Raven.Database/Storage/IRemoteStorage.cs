using System;

namespace Raven.Database.Storage
{
    public interface IRemoteStorage : IDisposable
    {
        void Batch(Action<IStorageActionsAccessor> action);
    }
}