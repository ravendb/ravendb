//-----------------------------------------------------------------------
// <copyright file="DatabaseBackupRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.FileSystem;

namespace Raven.Abstractions.Data
{
	public class FilesystemBackupRequest
	{
		public string BackupLocation { get; set; }
        public FileSystemDocument FileSystemDocument { get; set; }
	}
}
