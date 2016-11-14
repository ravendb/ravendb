//-----------------------------------------------------------------------
// <copyright file="RestoreRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.NewClient.Abstractions.Data
{
    public abstract class AbstractRestoreRequest
    {
        /// <summary>
        /// Path to directory where backup lies.
        /// </summary>
        public string BackupLocation { get; set; }
        
        [Obsolete]
        public string RestoreLocation { get { return BackupLocation; } set { BackupLocation = value; } }

        /// <summary>
        /// Path to directory where journals lies (if null, then default location will be assumed).
        /// </summary>
        public string JournalsLocation { get; set; }

        /// <summary>
        /// Path to directory where indexes lies (if null, then default location will be assumed).
        /// </summary>
        public string IndexesLocation { get; set; }

        /// <summary>
        /// Indicates if defragmentation should take place after restore.
        /// </summary>
        public bool Defrag { get; set; }

        /// <summary>
        /// Maximum number of seconds to wait for restore to start (only one restore can run simultaneously). If exceeded, then status code 503 (Service Unavailable) will be returned.
        /// </summary>
        public int? RestoreStartTimeout { get; set; }
    }
}
