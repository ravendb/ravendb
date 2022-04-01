using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Sparrow.Utils;
using Xunit.Sdk;
using ExceptionAggregator = Raven.Server.Utils.ExceptionAggregator;

namespace Tests.Infrastructure.Utils;

public class SessionTester
{
    private readonly IDocumentStore _documentStore;
    private DatabaseRecord _databaseRecord;

    public SessionTester(IDocumentStore documentStore)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Minor, "This helper is needed because not all session operations take into account sharding - get attachment, time series, counter");

        _documentStore = documentStore;
    }

    public async Task AssertAllAsync(Action<UniqueDatabaseInstanceKey, IDocumentSession> assert)
    {
        await foreach (var (key, result) in GetSessionsAsync())
        {
            try
            {
                assert(key, result);
            }
            catch (XunitException e)
            {
                throw new InvalidOperationException($"Assertion failed for '{key}'.", e);
            }
        }
    }

    public async Task AssertOneAsync(Action<UniqueDatabaseInstanceKey, IDocumentSession> assert)
    {
        var ea = new ExceptionAggregator("Assertion failed for all");

        var succeededAtLeastOnce = false;

        await foreach (var (key, result) in GetSessionsAsync())
        {
            try
            {
                assert(key, result);

                succeededAtLeastOnce = true;
            }
            catch (XunitException e)
            {
                ea.Execute(() => throw new InvalidOperationException($"Assertion failed for '{key}'.", e));
            }
        }

        if (succeededAtLeastOnce == false)
            ea.ThrowIfNeeded();
    }

    private async IAsyncEnumerable<(UniqueDatabaseInstanceKey Key, IDocumentSession Session)> GetSessionsAsync()
    {
        _databaseRecord ??= await _documentStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(_documentStore.Database));

        if (_databaseRecord.IsSharded)
        {
            for (var i = 0; i < _databaseRecord.Shards.Length; i++)
            {
                var shardTopology = _databaseRecord.Shards[i];

                foreach (string member in shardTopology.Members)
                {
                    var key = new UniqueDatabaseInstanceKey(member).ForShard(i);

                    var session = _documentStore.OpenSession(ShardHelper.ToShardName(_documentStore.Database, i));

                    yield return (key, session);
                }
            }

            yield break;
        }

        yield return (new UniqueDatabaseInstanceKey(_databaseRecord.Topology.Members.First()), _documentStore.OpenSession());
    }
}
