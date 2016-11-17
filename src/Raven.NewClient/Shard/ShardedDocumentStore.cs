//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Changes;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Util;

namespace Raven.NewClient.Client.Shard
{
    /// <summary>
    /// Implements a sharded document store
    /// Hiding most sharding details behind this and the <see cref="ShardedDocumentSession"/> gives you the ability to use
    /// sharding without really thinking about this too much
    /// </summary>
    public class ShardedDocumentStore 
    {
        
    }
}
