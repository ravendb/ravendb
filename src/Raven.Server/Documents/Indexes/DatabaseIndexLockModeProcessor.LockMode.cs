using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;

namespace Raven.Server.Documents.Indexes;

public class DatabaseIndexLockModeProcessor : AbstractIndexLockModeProcessor
{
    private readonly DocumentDatabase _database;

    public DatabaseIndexLockModeProcessor(DocumentDatabase database)
        : base(database.ServerStore)
    {
        _database = database;
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
