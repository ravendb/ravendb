using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron.Impl;

namespace Voron
{
    public unsafe class GlobalValidationBehavior
    {
        private const string ValidationThreadName = "Voron Validator Thread";

        private struct PageValidationRequest
        {
            public readonly StorageEnvironment Owner;
            public readonly LowLevelTransaction Transaction;
            public readonly long PageNumber;

            public PageValidationRequest(StorageEnvironment owner, LowLevelTransaction tx, long pageNumber)
            {
                Owner = owner;
                Transaction = tx;
                PageNumber = pageNumber;
            }
        }


        internal static readonly Lazy<GlobalValidationBehavior> GlobalValidator = new(() =>
        {
            var validator = new GlobalValidationBehavior();
            var thread = new Thread(validator.VoronValidator)
            {
                IsBackground = true,
                Name = ValidationThreadName
            };
            thread.Start();
            return validator;
        });

        public void Validate(StorageEnvironment owner, LowLevelTransaction tx, long pageNumber, PageHeader* header)
        {
            if (_requestQueue != null && tx.Flags.HasFlag(TransactionFlags.Read))
            {
                // This is a very important optimization for the case where warmup is needed for querying.
                // The cost of validation is high and therefore it makes sense to just offload it to a different
                // process and not stop the currently executing read transaction.
                _requestQueue.TryAdd(new PageValidationRequest(owner, tx, pageNumber));
            }
            else
            {
                // This is an entirely different deal because we have to deal with concurrency. We have to do the
                // validation of the data checksum before the actual transaction starts the process of committing, and
                // therefore prevent the transaction to advance to the commit stage if we haven't check the source
                // data. This is why write transactions will still have to execute the validation of pages coming 
                // from disk inline. 
                owner.ValidatePageChecksum(pageNumber, header);
            }
        }

        private readonly Logger _log = LoggingSource.Instance.GetLogger<GlobalPrefetchingBehavior>("Global Validator");

        private BlockingCollection<PageValidationRequest> _requestQueue;

        private void VoronValidator()
        {
            NativeMemory.EnsureRegistered();

            _requestQueue = new BlockingCollection<PageValidationRequest>();

            // We want this to always run, even if we dispose / create new storage env, this is 
            // static for the life time of the process, and environments will register / unregister from
            // it as needed

            try
            {
                while (true)
                {
                    if (_requestQueue.TryTake(out var request, TimeSpan.FromMinutes(5)) == false)
                        continue;

                    try
                    {
                        using var transaction = request.Owner.ReadTransaction();
                        var page = transaction.LowLevelTransaction.GetPage(request.PageNumber);
                        request.Owner.ValidatePageChecksum(page.PageNumber, (PageHeader*)page.Pointer);
                        transaction.Commit();
                    }
                    catch (InvalidDataException e)
                    {
                        // If it fails as we would expect, we will just kill the storage environment.
                        // It is important to note that this is a rare event, and will require external
                        // support to fix it as data might be corrupt.
                        request.Owner.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                    }
                }
            }
            catch (Exception e)
            {
                if (_log.IsOperationsEnabled)
                {
                    _log.Operations("Catastrophic failure in Voron validator ", e);
                }

                // Note that we intentionally don't have error handling here.
                // If this code throws an exception that bubbles up to here, we dont WANT the process
                // to die, since we can degrade gracefully even if the validator thread dies.
            }
            finally
            {
                // EVEN if it dies, then we need to be able to degrade 'gracefully'. We accomplish this 
                // by removing the blocking queue and force the validation to happen in the calling thread
                // context.
                _requestQueue = null;
            }
            // ReSharper disable once FunctionNeverReturns
        }
    }
}
