//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Database.Util;
using Raven.Tests.Helpers;
using System.Diagnostics;

namespace Raven.Tests
{
	using Raven.Abstractions.Util.Encryptors;

	public class RavenTest : RavenTestBase
	{
		static RavenTest()
		{
			LogManager.RegisterTarget<DatabaseMemoryTarget>();
		}

		public RavenTest()
		{
			SystemTime.UtcDateTime = () => DateTime.UtcNow;
		}

		protected void Consume(object o)
		{
			
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