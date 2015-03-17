//-----------------------------------------------------------------------
// <copyright file="DatabaseDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Database.Config
{
	public class GlobalSettingsDocument
	{
		/// <summary>
		/// Global settings (unsecured).
		/// </summary>
		public Dictionary<string, string> Settings { get; set; }

		/// <summary>
		/// Global settings (secured).
		/// </summary>
		public Dictionary<string, string> SecuredSettings { get; set; }

        public GlobalSettingsDocument()
		{
			Settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
			SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		}
	}
}