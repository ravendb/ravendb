using System;
using System.Threading;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    internal class SingleShardDatabaseSmugglerForBackup : SingleShardDatabaseSmuggler
    {
        public SingleShardDatabaseSmugglerForBackup(DocumentDatabase database, ISmugglerSource source, ISmugglerDestination destination, SystemTime time, JsonOperationContext context, DatabaseSmugglerOptionsServerSide options, SmugglerResult result = null, Action<IOperationProgress> onProgress = null, CancellationToken token = default) : 
            base(database, source, destination, time, context, options, result, onProgress, token)
        {
            _lastShard = true;
        }
    }
}
