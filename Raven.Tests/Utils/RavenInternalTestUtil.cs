// -----------------------------------------------------------------------
//  <copyright file="RavenInternalTestUtil.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Utils
{
	public class RavenInternalTestUtil
	{
		public static void OpenTestsNotSubclassingRavenTest()
		{
			var array = typeof(RavenTest).Assembly.GetTypes()
										  .Where(t => t.GetMethods().Any(m => m.GetCustomAttributes(typeof(FactAttribute), true).Length > 0))
										  .Where(t => typeof(RavenTestBase).IsAssignableFrom(t) == false)
										  .ToArray();

			Console.WriteLine("Count: " + array.Length);
			var arguments = new StringBuilder("/edit ");
			foreach (var type in array)
			{
				var fileInfos = new DirectoryInfo(@"C:\Work\RavenDB-New3\Raven.Tests").GetFiles(type.Name + ".cs", SearchOption.AllDirectories);
				foreach (var fileInfo in fileInfos)
				{
					var name = Path.Combine(fileInfo.Directory.FullName, fileInfo.Name);
					arguments.Append(name);
					arguments.Append(" ");
					break;
				}
			}

			var devenv = @"C:\Program Files (x86)\Microsoft Visual Studio 12.0\Common7\IDE\devenv.exe";
			Process.Start(new ProcessStartInfo(devenv, arguments.ToString()));
		} 
	}
}