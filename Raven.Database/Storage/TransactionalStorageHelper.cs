// -----------------------------------------------------------------------
//  <copyright file="TransactionalStorageHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Microsoft.Isam.Esent.Interop;

using Voron.Exceptions;

namespace Raven.Database.Storage
{
    public static class TransactionalStorageHelper
    {
        public static bool IsWriteConflict(Exception exception)
        {
            if (IsEsentWriteConflict(exception))
                return true;

            return IsVoronWriteConflict(exception);
        }

        private static bool IsVoronWriteConflict(Exception exception)
        {
            return exception is ConcurrencyException;
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
