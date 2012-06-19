// -----------------------------------------------------------------------
//  <copyright file="StressTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Globalization;
using System.Net.NetworkInformation;
using System.Xml;
using NLog;
using NLog.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using System.Linq;

namespace Raven.StressTests
{
	public class StressTest
	{
		public bool PrintLog { get; set; }

		public StressTest()
		{
			PrintLog = false;
			SetupLogging();
		}

		protected void Run<T>(Action<T> action, int iterations = 1000) where T : new()
		{
			var i = 0;
			try
			{
				for (; i < iterations; i++)
				{
					RunTest(action, i);
				}
			}
			catch
			{
				Console.WriteLine("Test failed on run #" + i);
				throw;
			}
		}

		protected void RunWithLog<T>(Action<T> action, int iterations = 1000) where T : new()
		{
			IOExtensions.DeleteDirectory("Logs");
			try
			{
				for (int i = 0; i < iterations; i++)
				{
					Environment.SetEnvironmentVariable("RunId", i.ToString(CultureInfo.InvariantCulture));
					Console.WriteLine("run #" + 1);
					RunTest(action, i);
				}
			}
			finally
			{
				LogManager.Flush();
			}
		}

		private static void RunTest<T>(Action<T> action, int i) where T : new()
		{
			var test = new T();
			action(test);

			var disposable = test as IDisposable;
			if (disposable != null)
				disposable.Dispose();

			var activeTcpListeners = IPGlobalProperties
				.GetIPGlobalProperties()
				.GetActiveTcpListeners();

			for (int j = 8079; j > 8020; j--)
			{
				if (activeTcpListeners.Any(x => x.Port == j))
				{
					throw new InvalidOperationException("Port " + j + " is still busy after the test");
				}
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
