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
		private Dictionary<string, string> attributes;
		public string Message { get; set; }
		public string Status { get; set; }
		public bool Error { get; set; }
		public bool IsCommercial
		{
			get { return Status.StartsWith("Commercial"); }
		}
		public bool ValidCommercialLicenseSeen { get; set; }

		public Dictionary<string, string> Attributes
		{
			get { return attributes ?? (attributes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)); }
			set { attributes = new Dictionary<string, string>(value, StringComparer.OrdinalIgnoreCase); }
		}
	}
}
