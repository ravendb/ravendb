using System;
using JetBrains.Annotations;
using Raven.Server.Documents.Changes;
using Raven.Server.Documents.Sharding.Changes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedDocumentsChanges Changes;

    public class ShardedDocumentsChanges : DocumentsChangesBase<ShardedChangesClientConnection, TransactionOperationContext>
    {
        private readonly ShardedDatabaseContext _context;

        public ShardedDocumentsChanges([NotNull] ShardedDatabaseContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }
    }
}
