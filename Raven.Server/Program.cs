//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Configuration.Install;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Principal;
using System.ServiceProcess;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Filter;
using log4net.Layout;
using log4net.Repository.Hierarchy;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl.Logging;
using Raven.Database.Server;
using Raven.Http;

namespace Raven.Server
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            if (Environment.UserInteractive)
            {
                try
                {
                    InteractiveRun(args);
                }
                catch (ReflectionTypeLoadException e)
                {
                    Console.WriteLine(e);
                    foreach (var loaderException in e.LoaderExceptions)
                    {
                        Console.WriteLine("- - - -");
                        Console.WriteLine(loaderException);
                    }
                    Environment.Exit(-1);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Environment.Exit(-1);
                }
            }
            else
            {
                // no try catch here, we want the exception to be logged by Windows
                ServiceBase.Run(new RavenService());
            }
        }

        private static void InteractiveRun(string[] args)
        {
            switch (GetArgument(args))
            {
                case "install":
                    AdminRequired(InstallAndStart, "/install");
                    break;
                case "uninstall":
                    AdminRequired(EnsureStoppedAndUninstall, "/uninstall");
                    break;
                case "start":
                    AdminRequired(StartService, "/start");
                    break;
                case "restart":
                    AdminRequired(RestartService, "/restart");
                    break;
                case "stop":
                    AdminRequired(StopService, "/stop");
                    break;
                case "restore":
                    if (args.Length != 3)
                    {
                        PrintUsage();
                        break;
                    }
                    RunRestoreOperation(args[1], args[2]);
                    break;
                case "debug":
                    RunInDebugMode(anonymousUserAccessMode: null, ravenConfiguration: new RavenConfiguration());
                    break;
                case "ram":
                    RunInDebugMode(anonymousUserAccessMode: AnonymousUserAccessMode.All, ravenConfiguration: new RavenConfiguration
                    {
                        RunInMemory = true,
                    });
                    break;
#if DEBUG
                case "test":
                    var dataDirectory = new RavenConfiguration().DataDirectory;
                    IOExtensions.DeleteDirectory(dataDirectory);

                    RunInDebugMode(anonymousUserAccessMode: AnonymousUserAccessMode.All, ravenConfiguration: new RavenConfiguration());
                    break;
#endif
                default:
                    PrintUsage();
                    break;
            }
        }

        private static void RunRestoreOperation(string backupLocation, string databaseLocation)
        {
            try
            {
                var ravenConfiguration = new RavenConfiguration();
                if(File.Exists(Path.Combine(backupLocation, "Raven.ravendb")))
                {
                    ravenConfiguration.DefaultStorageTypeName =
                        "Raven.Storage.Managed.TransactionalStorage, Raven.Storage.Managed";
                }
                else if(Directory.Exists(Path.Combine(backupLocation, "new")))
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
                    Arguments = cmdLine,
                    FileName = Assembly.GetExecutingAssembly().Location,
                    Verb = "runas",
                });
                if (process != null)
                    process.WaitForExit();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private static string GetArgument(string[] args)
        {
            if (args.Length == 0)
                return "debug";
            if (args[0].StartsWith("/") == false)
                return "help";
            return args[0].Substring(1);
        }

        private static void RunInDebugMode(AnonymousUserAccessMode? anonymousUserAccessMode, RavenConfiguration ravenConfiguration)
        {
        	ConfigureDebugLogging();

        	NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(ravenConfiguration.Port);
            if (anonymousUserAccessMode.HasValue)
                ravenConfiguration.AnonymousUserAccessMode = anonymousUserAccessMode.Value;
            while (RunServer(ravenConfiguration))
            {
                
            }
        }

    	private static void ConfigureDebugLogging()
    	{
			var loggerRepository = LogManager.GetRepository(typeof(HttpServer).Assembly);
			
			var patternLayout = new PatternLayout(PatternLayout.DefaultConversionPattern);
    		var consoleAppender = new ConsoleAppender
    		                      	{
    		                      		Layout = patternLayout,
    		                      	};
    		consoleAppender.ActivateOptions();
    		((Logger)loggerRepository.GetLogger(typeof(HttpServer).FullName)).AddAppender(consoleAppender);
    		var fileAppender = new RollingFileAppender
    		                   	{
    		                   		AppendToFile = false,
    		                   		File = "Raven.Server.log",
    		                   		Layout = patternLayout,
    		                   		MaxSizeRollBackups = 3,
    		                   		MaximumFileSize = "1024KB",
    		                   		StaticLogFileName = true,
									LockingModel = new FileAppender.MinimalLock()
    		                   	};
    		fileAppender.ActivateOptions();

    		var asyncBufferingAppender = new AsyncBufferingAppender();
    		asyncBufferingAppender.AddAppender(fileAppender);

    		((Hierarchy) loggerRepository).Root.AddAppender(asyncBufferingAppender);
    		loggerRepository.Configured = true;
    	}

    	private static bool RunServer(RavenConfiguration ravenConfiguration)
        {
            using (new RavenDbServer(ravenConfiguration))
            {
                var path = Path.Combine(Environment.CurrentDirectory, "default.raven");
                if (File.Exists(path))
                {
                    Console.WriteLine("Loading data from: {0}", path);
                    Smuggler.Smuggler.ImportData(ravenConfiguration.ServerUrl, path);
                }

                Console.WriteLine("Raven is ready to process requests. Build {0}, Version {1}", DocumentDatabase.BuildVersion, DocumentDatabase.ProductVersion);
                Console.WriteLine("Data directory: {0}, HostName: {1} Port: {2}", ravenConfiguration.DataDirectory, ravenConfiguration.HostName ?? "<any>", ravenConfiguration.Port);
                Console.WriteLine("Press the enter key to stop the server or enter 'cls' and then enter to clear the log");
                while (true)
                {
                    var readLine = Console.ReadLine() ?? "";
                    switch (readLine.ToLowerInvariant())
                    {
                        case "cls":
                            Console.Clear();
                            break;
                        case "reset":
                            Console.Clear();
                            return true;
                        default:
                            return false;
                    }
                }
            }
        }

        private static void PrintUsage()
        {
            Console.WriteLine(
                @"
Raven DB
Document Database for the .Net Platform
----------------------------------------
Copyright (C) 2010 - Hibernating Rhinos
----------------------------------------
Command line ptions:
    Raven.Server             - with no args, starts Raven in local server mode
    Raven.Server /install    - installs and starts the Raven service
    Raven.Server /uninstall  - stops and uninstalls the Raven service
    Raven.Server /start		- starts the previously installed Raven service
    Raven.Server /stop		- stops the previously installed Raven service
    Raven.Server /restart	- restarts the previously installed Raven service
	Raven.Server /restore [backup location] [new database location]
						- restore a previously backed up database to the new
						  location
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
                var startController = new ServiceController(ProjectInstaller.SERVICE_NAME);
                startController.Start();
            }
        }

        private static bool ServiceIsInstalled()
        {
            return (ServiceController.GetServices().Count(s => s.ServiceName == ProjectInstaller.SERVICE_NAME) > 0);
        }
    }
}
