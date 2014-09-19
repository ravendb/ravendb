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
		public string FilesystemName { get; set; }

        public string FilesystemLocation { get; set; }
	}
}