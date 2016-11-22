//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Document.Async;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Linq;
using Raven.NewClient.Json.Linq;
using Raven.NewClient.Client.Data.Queries;
using System.Threading.Tasks;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Data;

namespace Raven.NewClient.Client.Shard
{
    /// <summary>
    /// Implements Unit of Work for accessing a set of sharded RavenDB servers
    /// </summary>
    public class AsyncShardedDocumentSession 
        {
        private readonly AsyncDocumentKeyGeneration asyncDocumentKeyGeneration;

        public AsyncShardedDocumentSession(string dbName, ShardedDocumentStore documentStore, DocumentSessionListeners listeners, Guid id,
                                           ShardStrategy shardStrategy, IDictionary<string, IAsyncDatabaseCommands> shardDbCommands)
        {
            
        }

       
    }
}
