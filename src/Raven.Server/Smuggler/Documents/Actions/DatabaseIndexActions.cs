using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Org.BouncyCastle.Security.Certificates;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Routing;
using Raven.Server.Smuggler.Documents.Data;

namespace Raven.Server.Smuggler.Documents.Actions;

public sealed class DatabaseIndexActions : IIndexActions
{
    private readonly AbstractIndexCreateController _controller;
    private readonly SystemTime _time;
    private readonly AbstractIndexCreateController.IndexBatchScope _batch;
    private readonly RavenConfiguration _configuration;

    public DatabaseIndexActions([NotNull] AbstractIndexCreateController controller, [NotNull] SystemTime time)
    {
        _controller = controller ?? throw new ArgumentNullException(nameof(controller));
        _time = time ?? throw new ArgumentNullException(nameof(time));
        _configuration = controller.GetDatabaseConfiguration();

        if (_controller.CanUseIndexBatch())
            _batch = _controller.CreateIndexBatch();
    }

    public async ValueTask WriteAutoIndexAsync(IndexDefinitionBaseServerSide indexDefinition, IndexType indexType, AuthorizationStatus authorizationStatus)
    {
        if (_configuration.Security.AuthenticationEnabled &&
            authorizationStatus == AuthorizationStatus.ValidUser)
        {
            throw new CertificateException(
                $"Could not import '{indexDefinition.Name}' index: Your existing certificate lacks the necessary permissions to create indexes.");
        }

        if (_batch != null)
        {
            await _batch.AddIndexAsync(indexDefinition, _source, _time.GetUtcNow(), RaftIdGenerator.DontCareId);
            await _batch.SaveIfNeeded();
            return;
        }

        await _controller.CreateIndexAsync(indexDefinition, RaftIdGenerator.DontCareId);
    }

    public async ValueTask WriteIndexAsync(IndexDefinition indexDefinition, AuthorizationStatus authorizationStatus)
    {
        if (_configuration.Security.AuthenticationEnabled &&
            authorizationStatus == AuthorizationStatus.ValidUser &&
            (indexDefinition.Type == IndexType.Map ||
             indexDefinition.Type == IndexType.MapReduce))
        {
            throw new CertificateException(
                $"Could not import '{indexDefinition.Name}' index: Your existing certificate lacks the necessary permissions to create indexes.");
        }

        if (_batch != null)
        {
            await _batch.AddIndexAsync(indexDefinition, _source, _time.GetUtcNow(), RaftIdGenerator.DontCareId);
            await _batch.SaveIfNeeded();
            return;
        }

        await _controller.CreateIndexAsync(indexDefinition, RaftIdGenerator.DontCareId, _source);
    }

    private const string _source = "Smuggler";

    public async ValueTask DisposeAsync()
    {
        if (_batch == null)
            return;

        await _batch.SaveAsync();
    }
}
