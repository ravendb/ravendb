using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Xml;
using NLog;
using NLog.Config;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using System.Linq;
using Raven.Database.Server;
using Raven.Tests.Storage.MultiThreaded;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			if (Directory.Exists("Logs"))
			{
				foreach (var file in Directory.GetFiles("Logs"))
				{
					File.Delete(file);
				}
			}
			SetupLogging();

			try
			{
				for (int i = 0; i < 1000; i++)
				{
					Environment.SetEnvironmentVariable("Run", i.ToString());
					Console.Clear();
					Console.WriteLine(i);
					using (var x = new PutAndBatchOperation())
						x.WhenUsingMuninInMemory();
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

			using (var stream = typeof(Program).Assembly.GetManifestResourceStream("Raven.Tryouts.DefaultLogging.config"))
			using (var reader = XmlReader.Create(stream))
			{
				LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}

		}
	}
}