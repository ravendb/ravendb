//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Configuration.Install;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Principal;
using System.ServiceProcess;
using System.Xml;
using System.Xml.Linq;
using NDesk.Options;
using NLog.Config;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Smuggler;

namespace Raven.Server
{
	public static class Program
	{
		private static void Main(string[] args)
		{
			HttpEndpointRegistration.RegisterHttpEndpointTarget();
			if (RunningInInteractiveMode())
			{
				try
				{
					InteractiveRun(args);
				}
				catch (ReflectionTypeLoadException e)
				{
					EmitWarningInRed();

					Console.WriteLine(e);
					foreach (var loaderException in e.LoaderExceptions)
					{
						Console.WriteLine("- - - -");
						Console.WriteLine(loaderException);
					}

					WaitForUserInputAndExitWithError();
				}
				catch (Exception e)
				{
					EmitWarningInRed();

					Console.WriteLine(e);

					WaitForUserInputAndExitWithError();
				}
			}
			else
			{
				// no try catch here, we want the exception to be logged by Windows
				ServiceBase.Run(new RavenService());
			}
		}

		private static bool RunningInInteractiveMode()
		{
			if (Type.GetType("Mono.Runtime") != null) // running on mono, which doesn't support detecting this
				return true;
			return Environment.UserInteractive;
		}

		private static void WaitForUserInputAndExitWithError()
		{
			Console.WriteLine("Press any key to continue...");
			Console.ReadKey(true);
			Environment.Exit(-1);
		}

		private static void EmitWarningInRed()
		{
			var old = Console.ForegroundColor;
			Console.ForegroundColor = ConsoleColor.Red;
			Console.WriteLine("A critical error occurred while starting the server. Please see the exception details bellow for more details:");
			Console.ForegroundColor = old;
		}

		private static void InteractiveRun(string[] args)
		{
			string backupLocation = null;
			string restoreLocation = null;
			Action actionToTake = null;
			bool launchBrowser = false;
			var ravenConfiguration = new RavenConfiguration();

			OptionSet optionSet = null;
			optionSet = new OptionSet
			{
				{"set={==}", "The configuration {0:option} to set to the specified {1:value}" , (key, value) =>
				{
					ravenConfiguration.Settings[key] = value;
					ravenConfiguration.Initialize();
				}},
				{"config=", "The config {0:file} to use", path => ravenConfiguration.LoadFrom(path)},
				{"install", "Installs the RavenDB service", key => actionToTake= () => AdminRequired(InstallAndStart, key)},
				{"service-name=", "The {0:service name} to use when installing or uninstalling the service, default to RavenDB", name => ProjectInstaller.SERVICE_NAME = name},
				{"uninstall", "Uninstalls the RavenDB service", key => actionToTake= () => AdminRequired(EnsureStoppedAndUninstall, key)},
				{"start", "Starts the RavenDB servce", key => actionToTake= () => AdminRequired(StartService, key)},
				{"restart", "Restarts the RavenDB service", key => actionToTake= () => AdminRequired(RestartService, key)},
				{"stop", "Stops the RavenDB service", key => actionToTake= () => AdminRequired(StopService, key)},
				{"ram", "Run RavenDB in RAM only", key =>
				{
					ravenConfiguration.Settings["Raven/RunInMemory"] = "true";
					ravenConfiguration.RunInMemory = true;
					actionToTake = () => RunInDebugMode(AnonymousUserAccessMode.All, ravenConfiguration, launchBrowser);		
				}},
				{"debug", "Runs RavenDB in debug mode", key => actionToTake = () => RunInDebugMode(null, ravenConfiguration, launchBrowser)},
				{"browser|launchbrowser", "After the server starts, launches the browser", key => launchBrowser = true},
				{"help", "Help about the command line interface", key =>
				{
					actionToTake = () => PrintUsage(optionSet);
				}},
				{"config-help", "Help about configuration options", key=>
				{
					actionToTake = PrintConfig;
				}},
				{"restore", 
					"Restores a RavenDB database from backup",
					key => actionToTake = () =>
					{
						if(backupLocation == null || restoreLocation == null)
						{
							throw new OptionException("when using restore, source and destination must be specified", "restore");
						}
						RunRestoreOperation(backupLocation, restoreLocation);
					}},
				{"dest=|destination=", "The {0:path} of the new new database", value => restoreLocation = value},
				{"src=|source=", "The {0:path} of the backup", value => backupLocation = value},
				{"encrypt-self-config", "Encrypt the RavenDB configuration file", file =>
						{
							actionToTake = () => ProtectConfiguration(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile);
				        }},
				{"encrypt-config=", "Encrypt the specified {0:configuration file}", file =>
						{
							actionToTake = () => ProtectConfiguration(file);
				        }},
				{"decrypt-self-config", "Decrypt the RavenDB configuration file", file =>
						{
							actionToTake = () => UnprotectConfiguration(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile);
				        }},
				{"decrypt-config=", "Decrypt the specified {0:configuration file}", file =>
						{
							actionToTake = () => UnprotectConfiguration(file);
				        }}
			};


			try
			{
				if (args.Length == 0) // we default to executing in debug mode 
					args = new[] { "--debug" };

				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				Console.WriteLine(e.Message);
				PrintUsage(optionSet);
				return;
			}

			if (actionToTake == null)
				actionToTake = () => RunInDebugMode(null, ravenConfiguration, launchBrowser);

			actionToTake();

		}

		private static void ProtectConfiguration(string file)
		{
			if (string.Equals(Path.GetExtension(file), ".config", StringComparison.InvariantCultureIgnoreCase))
				file = Path.GetFileNameWithoutExtension(file);

			var configuration = ConfigurationManager.OpenExeConfiguration(file);
			var names = new[] {"appSettings", "connectionStrings"};

			foreach (var section in names.Select(configuration.GetSection))
			{
				section.SectionInformation.ProtectSection("RsaProtectedConfigurationProvider");
				section.SectionInformation.ForceSave = true;
			}

			configuration.Save(ConfigurationSaveMode.Full);
		}

		private static void UnprotectConfiguration(string file)
		{
			if (string.Equals(Path.GetExtension(file), ".config", StringComparison.InvariantCultureIgnoreCase))
				file = Path.GetFileNameWithoutExtension(file);

			var configuration = ConfigurationManager.OpenExeConfiguration(file);
			var names = new[] { "appSettings", "connectionStrings" };

			foreach (var section in names.Select(configuration.GetSection))
			{
				section.SectionInformation.UnprotectSection();
				section.SectionInformation.ForceSave = true;
			}
			configuration.Save(ConfigurationSaveMode.Full);
		}

		private static void PrintConfig()
		{
			Console.WriteLine(
				@"
Raven DB
Document Database for the .Net Platform
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Configuration options:
",
				SystemTime.UtcNow.Year);

			foreach (var configOptionDoc in ConfigOptionDocs.OptionsDocs)
			{
				Console.WriteLine(configOptionDoc);
				Console.WriteLine();
			}
		}

		private static void RunRestoreOperation(string backupLocation, string databaseLocation)
		{
			try
			{
				var ravenConfiguration = new RavenConfiguration();
				if (File.Exists(Path.Combine(backupLocation, "Raven.ravendb")))
				{
					ravenConfiguration.DefaultStorageTypeName =
						"Raven.Storage.Managed.TransactionalStorage, Raven.Storage.Managed";
				}
				else if (Directory.Exists(Path.Combine(backupLocation, "new")))
				{
					ravenConfiguration.DefaultStorageTypeName = "Raven.Storage.Esent.TransactionalStorage, Raven.Storage.Esent";

				}
				DocumentDatabase.Restore(ravenConfiguration, backupLocation, databaseLocation);
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}

		private static void AdminRequired(Action actionThatMayRequiresAdminPrivileges, string cmdLine)
		{
			var principal = new WindowsPrincipal(WindowsIdentity.GetCurrent());
			if (principal.IsInRole(WindowsBuiltInRole.Administrator) == false)
			{
				if (RunAgainAsAdmin(cmdLine))
					return;
			}
			actionThatMayRequiresAdminPrivileges();
		}

		private static bool RunAgainAsAdmin(string cmdLine)
		{
			try
			{
				var process = Process.Start(new ProcessStartInfo
				{
					Arguments = "--" + cmdLine,
					FileName = Assembly.GetExecutingAssembly().Location,
					Verb = "runas",
				});
				process.WaitForExit();
				return true;
			}
			catch (Exception)
			{
				return false;
			}
		}

		private static void RunInDebugMode(AnonymousUserAccessMode? anonymousUserAccessMode, RavenConfiguration ravenConfiguration, bool launchBrowser)
		{
			ConfigureDebugLogging();

			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(ravenConfiguration.Port);
			if (anonymousUserAccessMode.HasValue)
				ravenConfiguration.AnonymousUserAccessMode = anonymousUserAccessMode.Value;
			while (RunServerInDebugMode(ravenConfiguration, launchBrowser))
			{
				launchBrowser = false;
			}
		}

		private static void ConfigureDebugLogging()
		{
			var nlogPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NLog.config");
			if (File.Exists(nlogPath))
				return;// that overrides the default config

			using (var stream = typeof(Program).Assembly.GetManifestResourceStream("Raven.Server.DefaultLogging.config"))
			using (var reader = XmlReader.Create(stream))
			{
				NLog.LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}
		}

		private static bool RunServerInDebugMode(RavenConfiguration ravenConfiguration, bool lauchBrowser)
		{
			var sp = Stopwatch.StartNew();
			using (var server = new RavenDbServer(ravenConfiguration))
			{
				sp.Stop();
				var path = Path.Combine(Environment.CurrentDirectory, "default.raven");
				if (File.Exists(path))
				{
					Console.WriteLine("Loading data from: {0}", path);
					new SmugglerApi(new SmugglerOptions(), new RavenConnectionStringOptions {Url = ravenConfiguration.ServerUrl}).ImportData(new SmugglerOptions {File = path});
				}

				Console.WriteLine("Raven is ready to process requests. Build {0}, Version {1}", DocumentDatabase.BuildVersion, DocumentDatabase.ProductVersion);
				Console.WriteLine("Server started in {0:#,#;;0} ms", sp.ElapsedMilliseconds);
				Console.WriteLine("Data directory: {0}", ravenConfiguration.RunInMemory ? "RAM" : ravenConfiguration.DataDirectory);
				Console.WriteLine("HostName: {0} Port: {1}, Storage: {2}", ravenConfiguration.HostName ?? "<any>",
					ravenConfiguration.Port,
					server.Database.TransactionalStorage.FriendlyName);
				Console.WriteLine("Server Url: {0}", ravenConfiguration.ServerUrl);

				if (lauchBrowser)
				{
					try
					{
						Process.Start(ravenConfiguration.ServerUrl);
					}
					catch (Exception e)
					{
						Console.WriteLine("Could not start browser: " + e.Message);
					}
				}
				return InteractiveRun();
			}
		}

		private static bool InteractiveRun()
		{
			bool? done = null;
			var actions = new Dictionary<string,Action>
			{
				{"cls", TryClearingConsole},
				{
					"reset", () =>
					{
						TryClearingConsole();
						done = true;
					}
				},
				{
					"gc", () =>
					{
						long before = Process.GetCurrentProcess().WorkingSet64;
						Console.WriteLine("Starting garbage collection, current memory is: {0:#,#.##;;0} MB", before / 1024d / 1024d);
						GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
						var after = Process.GetCurrentProcess().WorkingSet64;
						Console.WriteLine("Done garbage collection, current memory is: {0:#,#.##;;0} MB, saved: {1:#,#.##;;0} MB", after / 1024d / 1024d,
										  (before - after) / 1024d / 1024d);
					}
					},
				{
					"q", () => done = false
				}
			};

			WriteInteractiveOptions(actions);
			while (true)
			{
				var readLine = Console.ReadLine() ?? "";

				Action value;
				if(actions.TryGetValue(readLine, out value) == false)
				{
					Console.WriteLine("Could not understand: {0}", readLine);
					WriteInteractiveOptions(actions);
					continue;
				}

				value();
				if (done != null)
					return done.Value;
			}
		}

		private static void TryClearingConsole()
		{
			try
			{
				Console.Clear();
			}
			catch (IOException)
			{
				// redirected output, probably, ignoring
			}
		}

		private static void WriteInteractiveOptions(Dictionary<string, Action> actions)
		{
			Console.WriteLine("Available commands: {0}", string.Join(", ", actions.Select(x => x.Key)));
		}

		private static void PrintUsage(OptionSet optionSet)
		{
			Console.WriteLine(
				@"
Raven DB
Document Database for the .Net Platform
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Command line ptions:",
				SystemTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);

			Console.WriteLine(@"
Enjoy...
");
		}

		private static void EnsureStoppedAndUninstall()
		{
			if (ServiceIsInstalled() == false)
			{
				Console.WriteLine("Service is not installed");
			}
			else
			{
				var stopController = new ServiceController(ProjectInstaller.SERVICE_NAME);

				if (stopController.Status == ServiceControllerStatus.Running)
					stopController.Stop();

				ManagedInstallerClass.InstallHelper(new[] { "/u", Assembly.GetExecutingAssembly().Location });
			}
		}

		private static void StopService()
		{
			var stopController = new ServiceController(ProjectInstaller.SERVICE_NAME);

			if (stopController.Status == ServiceControllerStatus.Running)
			{
				stopController.Stop();
				stopController.WaitForStatus(ServiceControllerStatus.Stopped);
			}
		}


		private static void StartService()
		{
			var stopController = new ServiceController(ProjectInstaller.SERVICE_NAME);

			if (stopController.Status != ServiceControllerStatus.Running)
			{
				stopController.Start();
				stopController.WaitForStatus(ServiceControllerStatus.Running);
			}
		}

		private static void RestartService()
		{
			var stopController = new ServiceController(ProjectInstaller.SERVICE_NAME);

			if (stopController.Status == ServiceControllerStatus.Running)
			{
				stopController.Stop();
				stopController.WaitForStatus(ServiceControllerStatus.Stopped);
			}
			if (stopController.Status != ServiceControllerStatus.Running)
			{
				stopController.Start();
				stopController.WaitForStatus(ServiceControllerStatus.Running);
			}

		}

		private static void InstallAndStart()
		{
			if (ServiceIsInstalled())
			{
				Console.WriteLine("Service is already installed");
			}
			else
			{
				ManagedInstallerClass.InstallHelper(new[] { Assembly.GetExecutingAssembly().Location });
				SetRecoveryOptions(ProjectInstaller.SERVICE_NAME);
				var startController = new ServiceController(ProjectInstaller.SERVICE_NAME);
				startController.Start();
			}
		}

		private static bool ServiceIsInstalled()
		{
			return (ServiceController.GetServices().Count(s => s.ServiceName == ProjectInstaller.SERVICE_NAME) > 0);
		}

		static void SetRecoveryOptions(string serviceName)
		{
			int exitCode;
			var arguments = string.Format("failure {0} reset= 500 actions= restart/60000", serviceName);
			using (var process = new Process())
			{
				var startInfo = process.StartInfo;
				startInfo.FileName = "sc";
				startInfo.WindowStyle = ProcessWindowStyle.Hidden;

				// tell Windows that the service should restart if it fails
				startInfo.Arguments = arguments;

				process.Start();
				process.WaitForExit();

				exitCode = process.ExitCode;

				process.Close();
			}

			if (exitCode != 0)
				throw new InvalidOperationException(
					"Failed to set the service recovery policy. Command: " + Environment.NewLine+ "sc " + arguments + Environment.NewLine + "Exit code: " + exitCode);
		} 
	}
}
