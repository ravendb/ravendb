// -----------------------------------------------------------------------
//  <copyright file="LicensingStatus.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class LicensingStatus
	{
		public string Message { get; set; }
		public string Status { get; set; }
		public bool Error { get; set; }
		public Dictionary<string, string> Attributes { get; set; }

		public LicensingStatus()
		{
			Attributes = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
		}
	}
}