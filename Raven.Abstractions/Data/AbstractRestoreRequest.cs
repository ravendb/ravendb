//-----------------------------------------------------------------------
// <copyright file="RestoreRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	public abstract class AbstractRestoreRequest
	{
        public string BackupLocation { get; set; }
        
        [Obsolete]
		public string RestoreLocation { get { return BackupLocation; } set { BackupLocation = value; } }

		public string DatabaseLocation { get; set; }

        public string JournalsLocation { get; set; }
        public string IndexesLocation { get; set; }
	    public bool Defrag { get; set; }
	}
}