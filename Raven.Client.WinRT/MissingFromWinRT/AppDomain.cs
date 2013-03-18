// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Windows.Storage;

namespace Raven.Client.WinRT.MissingFromWinRT
{
	public sealed class AppDomain
	{
		public static AppDomain CurrentDomain { get; private set; }

		static AppDomain()
		{
			CurrentDomain = new AppDomain();
		}

		public IEnumerable<Assembly> GetAssemblies()
		{
			return GetAssemblyListAsync().Result;
		}

		private async Task<IEnumerable<Assembly>> GetAssemblyListAsync()
		{
			var folder = Windows.ApplicationModel.Package.Current.InstalledLocation;

			var files = await folder.GetFilesAsync();
			return IterateOverFiles(files);
		}

		private IEnumerable<Assembly> IterateOverFiles(IEnumerable<StorageFile> files)
		{
			foreach (var file in files)
			{
				if (file.FileType == ".dll" || file.FileType == ".exe")
				{
					var name = new AssemblyName { Name = file.Name };
					Assembly asm = Assembly.Load(name);
					yield return asm;
				}
			}
		}
	}
}