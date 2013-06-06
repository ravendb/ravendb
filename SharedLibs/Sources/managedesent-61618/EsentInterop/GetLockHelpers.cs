// --------------------------------------------------------------------------------------------------------------------
// <copyright file="GetLockHelpers.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Helper methods for JetMakeKey.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System.Diagnostics;

    /// <summary>
    /// Helper methods for the ESENT API. These wrap JetMakeKey.
    /// </summary>
    public static partial class Api
    {
        /// <summary>
        /// Explicitly reserve the ability to update a row, write lock, or to explicitly prevent a row from
        /// being updated by any other session, read lock. Normally, row write locks are acquired implicitly as a
        /// result of updating rows. Read locks are usually not required because of record versioning. However,
        /// in some cases a transaction may desire to explicitly lock a row to enforce serialization, or to ensure
        /// that a subsequent operation will succeed. 
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to use. A lock will be acquired on the current record.</param>
        /// <param name="grbit">Lock options, use this to specify which type of lock to obtain.</param>
        /// <returns>
        /// True if the lock was obtained, false otherwise. An exception is thrown if an unexpected
        /// error is encountered.
        /// </returns>
        public static bool TryGetLock(JET_SESID sesid, JET_TABLEID tableid, GetLockGrbit grbit)
        {
            var err = (JET_err)Impl.JetGetLock(sesid, tableid, grbit);
            if (JET_err.WriteConflict == err)
            {
                return false;
            }

            Api.Check((int)err);
            Debug.Assert(err >= JET_err.Success, "Exception should have been thrown in case of error");
            return true;
        }
    }
}