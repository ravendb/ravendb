using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.BulkInsert;

namespace Raven.Client.Documents.BulkInsert;

public abstract class BulkInsertOperationBase<T>
{
    protected Task BulkInsertExecuteTask;

    protected long OperationId = -1;

    protected int MaxSizeInBuffer = 1024 * 1024;

    protected abstract bool HasStream { get; }

    public abstract Task StoreAsync(T entity, string id);

    protected async Task ExecuteBeforeStore()
    {
        if (HasStream == false)
        {
            await WaitForId().ConfigureAwait(false);
            await EnsureStreamAsync().ConfigureAwait(false);
        }

        if (BulkInsertExecuteTask.IsFaulted)
        {
            try
            {
                await BulkInsertExecuteTask.ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await ThrowBulkInsertAborted(e).ConfigureAwait(false);
            }
        }
    }

    protected async Task ThrowBulkInsertAborted(Exception e, Exception flushEx = null)
    {
        var errors = new List<Exception>(3);

        try
        {
            var error = await GetExceptionFromOperation().ConfigureAwait(false);

            if (error != null)
                errors.Add(error);
        }
        catch (Exception exceptionFromOperation)
        {
            errors.Add(exceptionFromOperation);
        }

        if (flushEx != null)
            errors.Add(flushEx);

        errors.Add(e);

        throw new BulkInsertAbortedException("Failed to execute bulk insert", new AggregateException(errors));
    }

    protected abstract Task WaitForId();

    protected abstract Task EnsureStreamAsync();

    protected abstract Task<BulkInsertAbortedException> GetExceptionFromOperation();
}
