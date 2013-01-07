//-----------------------------------------------------------------------
// <copyright file="Server2003Param.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Server2003
{
    /// <summary>
    /// System parameters that have been added to the Windows Server 2003 version of ESENT.
    /// </summary>
    public static class Server2003Param
    {
        /// <summary>
        /// The full path to each database is persisted in the transaction logs
        /// at run time. Ordinarily, these databases must remain at the original
        /// location for transaction replay to function correctly. This
        /// parameter can be used to force crash recovery or a restore operation
        /// to look for the databases referenced in the transaction log in the
        /// specified folder.
        /// </summary>
        public const JET_param AlternateDatabaseRecoveryPath = (JET_param)113;
    }
}