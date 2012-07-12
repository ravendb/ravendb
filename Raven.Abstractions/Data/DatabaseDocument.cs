//-----------------------------------------------------------------------
// <copyright file="DatabaseDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class DatabaseDocument
	{
		public string Id { get; set; }
		public Dictionary<string, string> Settings { get; set; }
		public Dictionary<string, string> SecuredSettings { get; set; }

		public DatabaseDocument()
		{
			Settings = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
			SecuredSettings = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
		}
	}
}