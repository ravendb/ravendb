//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using NLog;
using Raven.Abstractions;
using Raven.Database.Util;
using Raven.Tests.Document;
using Raven.Tests.Helpers;

namespace Raven.Tests
{
	public class RavenTest : RavenTestBase
	{
		static RavenTest()
		{
			File.Delete("test.log");
		}

		public RavenTest()
		{
			DatabaseMemoryTarget databaseMemoryTarget = null;
			if (LogManager.Configuration != null && LogManager.Configuration.AllTargets != null)
			{
				databaseMemoryTarget = LogManager.Configuration.AllTargets.OfType<DatabaseMemoryTarget>().FirstOrDefault();
			}
			if (databaseMemoryTarget != null)
			{
				databaseMemoryTarget.ClearAll();
			}

			SystemTime.UtcDateTime = () => DateTime.UtcNow;
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
			var startTime = SystemTime.UtcNow;
			action.Invoke();
			var timeTaken = SystemTime.UtcNow.Subtract(startTime);
			Console.WriteLine("Time take (ms)- " + timeTaken.TotalMilliseconds);
			return timeTaken.TotalMilliseconds;
		}
	}
}