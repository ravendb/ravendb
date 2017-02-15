using System;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Files
{
    public class FilesTransactionsMerger
    {
        private readonly FilesContextPool _pool;

        public abstract class MergedTransactionCommand
        {
            public abstract void Execute(FilesOperationContext context);
            public readonly TaskCompletionSource<object> TaskCompletionSource = new TaskCompletionSource<object>();
            public Exception Exception;
        }

        public FilesTransactionsMerger(FilesContextPool pool)
        {
            _pool = pool;
        }

        public Task Enqueue(MergedTransactionCommand command)
        {
            FilesOperationContext context;
            using (_pool.AllocateOperationContext(out context))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    command.Execute(context);
                    tx.Commit();
                }
            }
            return Task.CompletedTask;
        }
    }
}