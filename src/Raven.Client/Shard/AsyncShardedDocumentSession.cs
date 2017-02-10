//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Document.Async;

namespace Raven.Client.Shard
{
    /// <summary>
    /// Implements Unit of Work for accessing a set of sharded RavenDB servers
    /// </summary>
    public class AsyncShardedDocumentSession 
        {
        private readonly AsyncDocumentKeyGeneration asyncDocumentKeyGeneration;

        public AsyncShardedDocumentSession(string dbName, ShardedDocumentStore documentStore, Guid id,
                                           ShardStrategy shardStrategy)
        {
            
        }

       
    }
}
