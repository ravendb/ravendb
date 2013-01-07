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

        /// <summary>
        /// Throttling of the database scan, in milliseconds.
        /// </summary>
        public const JET_param DbScanThrottle = (JET_param)170;

        /// <summary>
        /// Minimum interval to repeat the database scan, in seconds.
        /// </summary>
        public const JET_param DbScanIntervalMinSec = (JET_param)171;

        /// <summary>
        /// Maximum interval to allow the database scan to finish, in seconds.
        /// </summary>
        public const JET_param DbScanIntervalMaxSec = (JET_param)172;
    }
}