using System;

namespace Raven.Database.Storage
{
    public interface IStalenessStorageActions
    {
        bool IsIndexStale(string name, DateTime? cutOff, string entityName);
        Tuple<DateTime, Guid> IndexLastUpdatedAt(string name);
    }
}