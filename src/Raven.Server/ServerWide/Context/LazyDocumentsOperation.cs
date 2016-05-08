using System;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class LazyDocumentsOperation : IDisposable
    {
        private readonly StorageEnvironment _storageEnvironment;
        private RavenTransaction _fictiveReadTransaction;
        private DocumentsContextPool contextPool;

        public LazyDocumentsOperation(StorageEnvironment storageEnvironment, DocumentsContextPool contextPool)
        {
            _storageEnvironment = storageEnvironment;
            // Add fictive read transaction to avoid ApplyLogsToDataFile
            DocumentsOperationContext fictiveContext;
            contextPool.AllocateOperationContext(out fictiveContext);
            _fictiveReadTransaction = fictiveContext.OpenReadTransaction();
        }

        public void Dispose()
        {            
            _storageEnvironment.WriteLazyBufferToFile();
            _fictiveReadTransaction?.Dispose();
        }
    }
}