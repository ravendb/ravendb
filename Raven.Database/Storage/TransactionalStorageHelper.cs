// -----------------------------------------------------------------------
//  <copyright file="TransactionalStorageHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;
using Voron.Exceptions;
using ConcurrencyException = Raven.Abstractions.Exceptions.ConcurrencyException;
using VoronConcurrencyException = Voron.Exceptions.ConcurrencyException;

namespace Raven.Database.Storage
{
    public static class TransactionalStorageHelper
    {
        public static bool IsWriteConflict(Exception exception, out Exception conflictException)
        {
            var ae = exception as AggregateException;
            if (ae == null)
            {
                conflictException = exception;

                return exception is ConcurrencyException ||
                       IsEsentWriteConflict(exception) || IsVoronWriteConflict(exception);
            }

            var isWriteConflict = false;
            conflictException = null;
            foreach (var innerException in ae.Flatten().InnerExceptions)
            {
                //if all inner exceptions are write conflicts
                isWriteConflict = innerException is ConcurrencyException || 
                    IsEsentWriteConflict(innerException) || IsVoronWriteConflict(innerException);

                if (isWriteConflict)
                {
                    conflictException = innerException;
                    break;
                }
            }

            return isWriteConflict;
        }

        private static bool IsVoronWriteConflict(Exception exception)
        {
            return exception is VoronConcurrencyException;
        }

        private static bool IsEsentWriteConflict(Exception exception)
        {
            var esentErrorException = exception as EsentErrorException;
            if (esentErrorException == null)
                return false;
            switch (esentErrorException.Error)
            {
                case JET_err.WriteConflict:
                case JET_err.SessionWriteConflict:
                case JET_err.WriteConflictPrimaryIndex:
                case JET_err.KeyDuplicate:
                    return true;
                default:
                    return false;
            }
        }

        public static bool IsOutOfMemoryException(Exception exception)
        {
            if (IsEsentOutOfMemoryException(exception))
                return true;

            return IsVoronOutOfMemoryException(exception);
        }

        private static bool IsVoronOutOfMemoryException(Exception exception)
        {
            return exception is ScratchBufferSizeLimitException;
        }

        private static bool IsEsentOutOfMemoryException(Exception exception)
        {
            var esentErrorException = exception as EsentErrorException;
            if (esentErrorException == null)
                return false;

            switch (esentErrorException.Error)
            {
                case JET_err.OutOfMemory:
                case JET_err.CurrencyStackOutOfMemory:
                case JET_err.SPAvailExtCacheOutOfMemory:
                case JET_err.VersionStoreOutOfMemory:
                case JET_err.VersionStoreOutOfMemoryAndCleanupTimedOut:
                    return true;
            }

            return false;
        }
    }
}
