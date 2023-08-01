using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents.Data;

namespace Raven.Server.Smuggler.Documents.Actions;

public sealed class DatabaseKeyValueActions : IKeyValueActions<long>
{
    private readonly ServerStore _serverStore;
    private readonly string _name;
    private readonly Dictionary<string, long> _identities;

    public DatabaseKeyValueActions(DocumentDatabase database)
    {
        _serverStore = database.ServerStore;
        _name = database.Name;
        _identities = new Dictionary<string, long>();
    }

    public DatabaseKeyValueActions(ServerStore server, string name)
    {
        _serverStore = server;
        _name = name;
        _identities = new Dictionary<string, long>();
    }

    public async ValueTask WriteKeyValueAsync(string key, long value)
    {
        const int batchSize = 1024;

        _identities[key] = value;

        if (_identities.Count < batchSize)
            return;

        await SendIdentitiesAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_identities.Count == 0)
            return;

        await SendIdentitiesAsync();
    }

    private async ValueTask SendIdentitiesAsync()
    {
        //fire and forget, do not hold-up smuggler operations waiting for Raft command
        await _serverStore.SendToLeaderAsync(new UpdateClusterIdentityCommand(_name, _identities, false, RaftIdGenerator.NewId()));

        _identities.Clear();
    }
}
