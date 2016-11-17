//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Listeners;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Shard;
using Raven.NewClient.Json.Linq;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// A query that is executed against sharded instances
    /// </summary>
    public class ShardedDocumentQuery<T> 
    {
        
    }
}
