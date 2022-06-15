using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Sparrow.Json;

namespace Raven.Server.ServerWide.Sharding;

public class RawShardingConfiguration
{
    private readonly ShardingConfiguration _materializedSharding;

    private readonly BlittableJsonReaderObject _sharding;
    private readonly JsonOperationContext _context;

    public RawShardingConfiguration([NotNull] JsonOperationContext context, [NotNull] BlittableJsonReaderObject sharding)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _sharding = sharding ?? throw new ArgumentNullException(nameof(sharding));
    }

    public RawShardingConfiguration(ShardingConfiguration sharding)
    {
        _materializedSharding = sharding;
    }

    public BlittableJsonReaderObject Raw
    {
        get
        {
            if (_sharding == null)
                throw new ArgumentNullException(nameof(_sharding));

            return _sharding;
        }
    }

    private string _shardedDatabaseId;

    public string ShardedDatabaseId
    {
        get
        {
            if (_materializedSharding != null)
                return _materializedSharding.ShardedDatabaseId;

            if (_shardedDatabaseId == null)
                _sharding.TryGet(nameof(ShardingConfiguration.ShardedDatabaseId), out _shardedDatabaseId);

            return _shardedDatabaseId;
        }
    }

    private Dictionary<int, ShardBucketMigration> _bucketMigrations;

    public Dictionary<int, ShardBucketMigration> BucketMigrations
    {
        get
        {
            if (_materializedSharding != null)
                return _materializedSharding.ShardBucketMigrations;

            if (_bucketMigrations == null)
            {
                _bucketMigrations = new Dictionary<int, ShardBucketMigration>();
                if (_sharding.TryGet(nameof(ShardingConfiguration.ShardBucketMigrations), out BlittableJsonReaderObject obj) && obj != null)
                {
                    var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                    for (var i = 0; i < obj.Count; i++)
                    {
                        obj.GetPropertyByIndex(i, ref propertyDetails);

                        if (propertyDetails.Value == null)
                            continue;

                        if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                            _bucketMigrations[int.Parse(propertyDetails.Name)] = JsonDeserializationCluster.BucketMigration(bjro);
                    }
                }
            }

            return _bucketMigrations;
        }
    }

    private DatabaseTopology[] _shards;

    public DatabaseTopology[] Shards
    {
        get
        {
            if (_materializedSharding != null)
                return _materializedSharding.Shards;

            if (_shards != null)
                return _shards;

            if (_sharding.TryGet(nameof(ShardingConfiguration.Shards), out BlittableJsonReaderArray array) == false || array == null)
                return null;

            _shards = new DatabaseTopology[array.Length];
            for (var index = 0; index < array.Length; index++)
            {
                var shard = (BlittableJsonReaderObject)array[index];
                _shards[index] = JsonDeserializationCluster.DatabaseTopology(shard);
            }

            return _shards;
        }
    }

    private List<ShardBucketRange> _shardBucketRanges;

    public List<ShardBucketRange> ShardBucketRanges
    {
        get
        {
            if (_materializedSharding != null)
                return _materializedSharding.ShardBucketRanges;

            if (_shardBucketRanges != null)
                return _shardBucketRanges;

            if (_sharding.TryGet(nameof(ShardingConfiguration.ShardBucketRanges), out BlittableJsonReaderArray array) == false || array == null)
                return null;

            _shardBucketRanges = new List<ShardBucketRange>(array.Length);
            for (var index = 0; index < array.Length; index++)
            {
                var shardAllocation = (BlittableJsonReaderObject)array[index];
                _shardBucketRanges.Add(JsonDeserializationCluster.ShardRangeAssignment(shardAllocation));
            }

            return _shardBucketRanges;
        }
    }
}
