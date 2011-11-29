// -----------------------------------------------------------------------
//  <copyright file="LicenseStatus.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Abstractions.Data
{
	public class LicenseStatus
	{
		public bool Error { get; set; }
		public string Status { get; set; }
		public string Message { get; set; }
	}
}