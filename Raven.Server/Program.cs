using System;
using System.Configuration.Install;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Principal;
using System.ServiceProcess;
using log4net.Appender;
using log4net.Config;
using log4net.Filter;
using log4net.Layout;
using Raven.Database;
using Raven.Database.Server;

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
                    RunRestoreOperation(args[0], args[1]);
                    break;
                case "debug":
                    RunInDebugMode(anonymousUserAccessMode: null);
                    break;
#if DEBUG
                case "test":
                    var dataDirectory = new RavenConfiguration().DataDirectory;
                    if (Directory.Exists(dataDirectory))
                        Directory.Delete(dataDirectory, true);

                    RunInDebugMode(anonymousUserAccessMode: AnonymousUserAccessMode.All);
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
                DocumentDatabase.Restore(new RavenConfiguration(), backupLocation, databaseLocation);
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

        private static void RunInDebugMode(AnonymousUserAccessMode? anonymousUserAccessMode)
        {
			var consoleAppender = new ConsoleAppender
			{
				Layout = new PatternLayout(PatternLayout.DefaultConversionPattern),
			};
			consoleAppender.AddFilter(new LoggerMatchFilter
			{
				AcceptOnMatch = true,
				LoggerToMatch = typeof(HttpServer).FullName
			});
			consoleAppender.AddFilter(new DenyAllFilter());
			BasicConfigurator.Configure(consoleAppender);
            var ravenConfiguration = new RavenConfiguration();
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(ravenConfiguration.Port);
            if (anonymousUserAccessMode.HasValue)
                ravenConfiguration.AnonymousUserAccessMode = anonymousUserAccessMode.Value;
            using (new RavenDbServer(ravenConfiguration))
            {
                var path = Path.Combine(Environment.CurrentDirectory, "default.raven");
                if (File.Exists(path))
                {
                    Console.WriteLine("Loading data from: {0}", path);
                    Smuggler.Smuggler.ImportData(ravenConfiguration.ServerUrl, path);
                }

                Console.WriteLine("Raven is ready to process requests.");
                Console.WriteLine("Data directory: {0}, HostName: {1} Port: {2}", ravenConfiguration.DataDirectory, ravenConfiguration.HostName ?? "<any>", ravenConfiguration.Port);
                Console.WriteLine("Press the enter key to stop the server or enter 'cls' and then enter to clear the log");
                while (true)
                {
                    var readLine = Console.ReadLine();
                    if (!"CLS".Equals(readLine, StringComparison.InvariantCultureIgnoreCase))
                        break;
                    Console.Clear();
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
    RavenDb             - with no args, starts Raven in local server mode
    RavenDb /install    - installs and starts the Raven service
    RavenDb /unisntall  - stops and uninstalls the Raven service
    RavenDb /start		- starts the previously installed Raven service
    RavenDb /stop		- stops the previously installed Raven service
    RavenDb /restart	- restarts the previously installed Raven service
	RavenDB /restore [backup location] [new database location]
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