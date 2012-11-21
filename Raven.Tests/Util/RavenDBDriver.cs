using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;

namespace Raven.Tests.Util
{
	[CLSCompliant(false)]
	public class RavenDBDriver : ProcessDriver, IDisposable
	{
		readonly string _shardName;
		readonly DocumentConvention _conventions;
		readonly string _dataDir;

		public string Url { get; private set; }

		public RavenDBDriver(string shardName, DocumentConvention conventions)
		{
			_shardName = shardName;
			_conventions = conventions;
			_dataDir = GetPath(shardName);
		}

		public void Start()
		{
			IOExtensions.DeleteDirectory(_dataDir);

			var exePath = GetPath("Raven.Server.Exe");

			if (!File.Exists(exePath))
			{
				throw new Exception("Could not find Raven.server.exe");
			}

			StartProcess(exePath, "--ram --set=Raven/Port==8079 --msgBox");

			Match match = WaitForConsoleOutputMatching(@"^Server Url: (http://.*/)\s*$");

			Url = match.Groups[1].Value;
		}

		public IDocumentStore GetDocumentStore()
		{
			var documentStore = new DocumentStore()
			{
				Identifier = _shardName,
				Url = this.Url,
				Conventions = _conventions
			};

			documentStore.Initialize();

			return documentStore;
		}

		public void TraceExistingOutput()
		{
			Console.WriteLine("Console output:");
			Console.WriteLine(_process.StandardOutput.ReadToEnd());
			Console.WriteLine("Error output:");
			Console.WriteLine(_process.StandardError.ReadToEnd());
		}

		public void Should_finish_without_error()
		{
			try
			{
				_process.StandardInput.Write("q\r\n");
			}
			catch (Exception)
			{
			}

			if (!_process.WaitForExit(10000))
				throw new Exception("RavenDB command-line server did not halt within 10 seconds of pressing enter.");

			string errorOutput = _process.StandardError.ReadToEnd();
			string output = _process.StandardOutput.ReadToEnd();

			if (!String.IsNullOrEmpty(errorOutput))
				throw new Exception("RavendB command-line server finished with error text: " + errorOutput + "\r\n" + output);

			if (_process.ExitCode != 0)
				throw new Exception("RavenDB command-line server finished with exit code: " + _process.ExitCode + " " + output);
		}

		protected override void Shutdown()
		{
			Should_finish_without_error();
		}

		protected string GetPath(string subFolderName)
		{
			string retPath = Path.GetDirectoryName(Assembly.GetAssembly(typeof(RavenDBDriver)).CodeBase);
			return Path.Combine(retPath, subFolderName).Substring(6); //remove leading file://
		}

	}
}
