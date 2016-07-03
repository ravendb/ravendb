using System;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class LazyTransactionsHandler : AdminDatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/lazy-transaction", "GET")]
        public Task CommitNonLazyTx()
        {
            var isLazyTxMode = GetBoolValueQueryString("mode") ?? false;
            var duration = GetTimeSpanQueryString("duration", required: false) ?? TimeSpan.FromDays(1);
            Database.LazyTransactionMode = isLazyTxMode;
            Database.LazyTransactionExpiration = DateTime.Now + duration;

            if (isLazyTxMode == false)
            {
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        tx.InnerTransaction.LowLevelTransaction.IsLazyTransaction = false;
                        // this non lazy transaction forces the journal to actually
                        // flush everything
                        tx.Commit();
                    }
                }
            }
            return Task.CompletedTask;
        }

    }
}
