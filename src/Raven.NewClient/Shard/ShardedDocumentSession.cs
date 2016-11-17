//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Reflection;
using System.Threading;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Linq;
using Raven.NewClient.Json.Linq;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Http;

namespace Raven.NewClient.Client.Shard
{
    /// <summary>
    /// Implements Unit of Work for accessing a set of sharded RavenDB servers
    /// </summary>
    public class ShardedDocumentSession 
    {
        
    }
}
