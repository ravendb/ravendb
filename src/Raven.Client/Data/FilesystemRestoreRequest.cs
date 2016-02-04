//-----------------------------------------------------------------------
// <copyright file="DatabaseRestoreRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
    public class FilesystemRestoreRequest : AbstractRestoreRequest
    {
        /// <summary>
        /// Indicates what should be the name of filesystem after restore. If null then name will be read from 'Filesystem.Document' found in backup.
        /// </summary>
        public string FilesystemName { get; set; }

        /// <summary>
        /// Path to the directory of a new filesystem. If null then default location will be assumed.
        /// </summary>
        public string FilesystemLocation { get; set; }
    }
}
