using System;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Voron.Impl;

namespace Raven.Server.Files
{
    public class FilesTransaction : RavenTransaction
    {
        private readonly FilesOperationContext _context;

        private bool _replaced;

        public FilesTransaction(FilesOperationContext context, Transaction transaction)
            : base(transaction)
        {
            _context = context;
        }

        public FilesTransaction BeginAsyncCommitAndStartNewTransaction()
        {
            _replaced = true;
            var tx = InnerTransaction.BeginAsyncCommitAndStartNewTransaction();
            return new FilesTransaction(_context, tx);
        }

        private bool _isDisposed = false;

        public override void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;

            if (_replaced == false)
            {
                if (_context.Transaction != null && _context.Transaction != this)
                    ThrowInvalidTransactionUsage();

                _context.Transaction = null;
            }

            base.Dispose();
        }

        private static void ThrowInvalidTransactionUsage()
        {
            throw new InvalidOperationException("There is a different transaction in context.");
        }
    }
}