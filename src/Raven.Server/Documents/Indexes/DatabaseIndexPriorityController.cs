using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes.Errors;

namespace Raven.Server.Documents.Indexes;

public class DatabaseIndexPriorityController : AbstractIndexPriorityController
{
    private readonly DocumentDatabase _database;

    public DatabaseIndexPriorityController([NotNull] DocumentDatabase database)
        : base(database.ServerStore)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override void ValidateIndex(string name, IndexPriority priority)
    {
        var index = _database.IndexStore.GetIndex(name);
        if (index == null)
            IndexDoesNotExistException.ThrowFor(name);

        if (index is FaultyInMemoryIndex faultyInMemoryIndex)
            faultyInMemoryIndex.SetPriority(priority); // this will throw proper exception
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
