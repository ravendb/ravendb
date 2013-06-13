//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Database.Util;
using Raven.Tests.Document;
using Raven.Tests.Helpers;
using System.Diagnostics;

namespace Raven.Tests
{
	public class RavenTest : RavenTestBase
	{
		public RavenTest()
		{
			SystemTime.UtcDateTime = () => DateTime.UtcNow;
			LogManager.ClearTargets();
		}

		protected void Consume(object o)
		{
			
		}

		public string GetPath(string subFolderName)
		{
			string retPath = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			return Path.Combine(retPath, subFolderName).Substring(6); // remove leading file://
		}

		public double Timer(Action action)
		{
			var timer = Stopwatch.StartNew();
			action.Invoke();
            timer.Stop();
            Console.WriteLine("Time take (ms)- " + timer.Elapsed.TotalMilliseconds);
            return timer.Elapsed.TotalMilliseconds;
		}
	}
}