using System;
using System.IO;
using System.Xml;
using NLog;
using NLog.Config;
using Raven.Bundles.Tests.Authorization.Bugs;
using Raven.Database.Server;

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
					using (var x = new LoadingSavedInfo())
						x.BugWhenSavingDocumentWithPreviousAuthorization_WithQuery();
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