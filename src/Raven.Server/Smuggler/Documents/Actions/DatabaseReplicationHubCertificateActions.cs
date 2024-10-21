using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents.Data;

namespace Raven.Server.Smuggler.Documents.Actions;

public sealed class DatabaseReplicationHubCertificateActions : IReplicationHubCertificateActions
{
    private readonly ServerStore _server;
    private readonly string _name;
    private readonly List<RegisterReplicationHubAccessCommand> _commands = new List<RegisterReplicationHubAccessCommand>();

    public DatabaseReplicationHubCertificateActions(DocumentDatabase database)
    {
        _server = database.ServerStore;
        _name = database.Name;
    }

    public DatabaseReplicationHubCertificateActions(ServerStore serverStore, string name)
    {
        _server = serverStore;
        _name = name;
    }

    public async ValueTask DisposeAsync()
    {
        if (_commands.Count == 0)
            return;

        await SendCommandsAsync();
    }

    public async ValueTask WriteReplicationHubCertificateAsync(string hub, ReplicationHubAccess access)
    {
        const int batchSize = 128;

        byte[] buffer = Convert.FromBase64String(access.CertificateBase64);
        using var cert = CertificateLoaderUtil.CreateCertificateFromAny(buffer);

        _commands.Add(new RegisterReplicationHubAccessCommand(_name, hub, access, cert, RaftIdGenerator.DontCareId));

        if (_commands.Count < batchSize)
            return;

        await SendCommandsAsync();
    }

    private async ValueTask SendCommandsAsync()
    {
        await _server.SendToLeaderAsync(new BulkRegisterReplicationHubAccessCommand
        {
            Commands = _commands,
            Database = _name,
            UniqueRequestId = RaftIdGenerator.DontCareId
        });

        _commands.Clear();
    }
}
