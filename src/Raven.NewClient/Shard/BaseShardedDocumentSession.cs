using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Document.Commands;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Linq;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Shard
{
    public abstract class BaseShardedDocumentSession<TDatabaseCommands> 
    {
        
    }
}
