// -----------------------------------------------------------------------
//  <copyright file="TransactionalStorageHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Voron.Exceptions;

namespace Raven.Database.Storage
{
    public static class TransactionalStorageHelper
    {
        public static bool IsWriteConflict(Exception exception)
        {
            return exception is ConcurrencyException;
        }

        public static bool IsOutOfMemoryException(Exception exception)
        {
            return exception is ScratchBufferSizeLimitException;
        }
    }
}
