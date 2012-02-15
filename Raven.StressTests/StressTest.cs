// -----------------------------------------------------------------------
//  <copyright file="StressTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Xml;
using NLog;
using NLog.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;

namespace Raven.StressTests
{
	public class StressTest
	{
		public StressTest()
		{
			IOExtensions.DeleteDirectory("Logs");
			SetupLogging();
		}

		protected void Run<T>(Action<T> action, int iterations = 1000) where T : new()
		{
			try
			{
				for (int i = 0; i < 1000; i++)
				{
					Environment.SetEnvironmentVariable("Run", i.ToString(CultureInfo.InvariantCulture));

					var test = new T();
					action(test);

					var disposable = test as IDisposable;
					if (disposable != null)
						disposable.Dispose();
				}
			}
			finally
			{
				LogManager.Flush();
			}
		}

		private static void SetupLogging()
		{
			HttpEndpointRegistration.RegisterHttpEndpointTarget();

			using (var stream = typeof(StressTest).Assembly.GetManifestResourceStream(typeof(StressTest).Namespace + ".DefaultLogging.config"))
			using (var reader = XmlReader.Create(stream))
			{
				LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}
		}
	}
}