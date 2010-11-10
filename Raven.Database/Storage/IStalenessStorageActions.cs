using System;

namespace Raven.Database.Storage
{
    public interface IStalenessStorageActions
    {
        bool IsIndexStale(string name, DateTime? cutOff, string entityName);
        DateTime IndexLastUpdatedAt(string name);
    }
}