//-----------------------------------------------------------------------
// <copyright file="Windows7Param.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Windows7
{
    /// <summary>
    /// System parameters that have been added to the Windows 7 version of ESENT.
    /// </summary>
    public static class Windows7Param
    {
        /// <summary>
        /// This parameter sets the number of logs that esent will defer database
        /// flushes for. This can be used to increase database recoverability if
        /// failures cause logfiles to be lost.
        /// </summary>
        public const JET_param WaypointLatency = (JET_param)153;

        /// <summary>
        /// This parameter is used to retrieve the chunk size of long-value
        /// (blob) data. Setting and retrieving data in multiples of this 
        /// size increases efficiency.
        /// </summary>
        public const JET_param LVChunkSizeMost = (JET_param)163;
    }
}