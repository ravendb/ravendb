// -----------------------------------------------------------------------
//  <copyright file="IISExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.DirectoryServices;
using System.Linq;
using Microsoft.Web.Administration;

namespace Raven.Setup.CustomActions
{
	public static class IISExtensions
	{
		private const string IISEntry = "IIS://localhost/W3SVC/";
		private const string Root = "/root";
		private const string Path = "Path";

		public static string PhysicalPath(this Site site)
		{
			var root = site.Applications.Single(a => a.Path == "/");
			var vRoot = root.VirtualDirectories.Single(v => v.Path == "/");

			return Environment.ExpandEnvironmentVariables(vRoot.PhysicalPath);
		}

		public static string PhysicalPath(this DirectoryEntry site)
		{
			using (var de = new DirectoryEntry(string.Format("{0}{1}{2}", IISEntry, site.Name, Root)))
			{
				return de.Properties[Path].Value.ToString();
			}
		} 
	}
}