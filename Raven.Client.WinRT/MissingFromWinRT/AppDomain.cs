// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Raven.Client.WinRT.MissingFromWinRT
{
	public sealed class AppDomain
	{
		public static AppDomain CurrentDomain { get; private set; }

		static AppDomain()
		{
			CurrentDomain = new AppDomain();
		}

		public Assembly[] GetAssemblies()
		{
			return GetAssemblyListAsync().Result.ToArray();
		}

		private async System.Threading.Tasks.Task<IEnumerable<Assembly>> GetAssemblyListAsync()
		{
			var folder = Windows.ApplicationModel.Package.Current.InstalledLocation;

			var files = await folder.GetFilesAsync();

			return files.Where(file => file.FileType == ".dll" || file.FileType == ".exe")
				.Select(file => new AssemblyName {Name = file.Name})
				.Select(Assembly.Load)
				.ToList();
		}
	}
}