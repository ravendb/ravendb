//-----------------------------------------------------------------------
// <copyright file="jet_snt.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Type of progress being reported.
    /// </summary>
    public enum JET_SNT
    {
        /// <summary>
        /// Callback for the beginning of an operation.
        /// </summary>
        Begin = 5,

        /// <summary>
        /// Callback for operation progress.
        /// </summary>
        Progress = 0,

        /// <summary>
        /// Callback for the completion of an operation.
        /// </summary>
        Complete = 6,

        /// <summary>
        /// Callback for failure during the operation.
        /// </summary>
        Fail = 3,

        /// <summary>
        /// Callback for recovery control.
        /// </summary>
        RecoveryStep = 8,
    }
}
