using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;

namespace Raven.Server.Documents.Indexes;

public class DatabaseIndexLockModeController : AbstractIndexLockModeController
{
    private readonly DocumentDatabase _database;

    public DatabaseIndexLockModeController([NotNull] DocumentDatabase database)
        : base(database.ServerStore)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override void ValidateIndex(string name, IndexLockMode mode)
    {
        var index = _database.IndexStore.GetIndex(name);
        if (index == null)
            IndexDoesNotExistException.ThrowFor(name);

        if (index.Type == IndexType.Faulty || index.Type.IsAuto())
        {
            index.SetLock(mode);  // this will throw proper exception
            return;
        }
    }

    protected override string GetDatabaseName()
    {
        return _database.Name;
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await _database.RachisLogIndexNotifications.WaitForIndexNotification(index, _database.ServerStore.Engine.OperationTimeout);
    }
}
