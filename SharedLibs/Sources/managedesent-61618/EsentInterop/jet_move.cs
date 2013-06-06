//-----------------------------------------------------------------------
// <copyright file="jet_move.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Offsets for JetMove.
    /// </summary>
    public enum JET_Move
    {
        /// <summary>
        /// Move the cursor to the first index entry.
        /// </summary>
        First = -2147483648,

        /// <summary>
        /// Move to the previous index entry.
        /// </summary>
        Previous = -1,

        /// <summary>
        /// Move to the next index entry.
        /// </summary>
        Next = 1,

        /// <summary>
        /// Move to the last index entry.
        /// </summary>
        Last = 0x7fffffff,
    }
}
