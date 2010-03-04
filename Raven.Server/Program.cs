using System;
using System.Diagnostics;
using System.Linq;
using System.Security.Principal;
using System.ServiceProcess;
using System.Configuration.Install;
using System.Reflection;

namespace Raven.Server
{
    class Program
    {
        static void Main(string[] args)
        {
            if (Environment.UserInteractive)
            {
                switch (GetArgument(args))
                {
                    case "install":
                        AdminRequired(InstallAndStart,"/install");
                        break;
                    case "uninstall":
                        AdminRequired(EnsureStoppedAndUninstall, "/uninstall");
                        break;
                    case "debug":
                        RunInDebugMode();
                        break;
                    default:
                        PrintUsage();
                        break;
                }
            }
            else
            {
                ServiceBase.Run(new RavenService());
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
                Process.Start(new ProcessStartInfo
                {
                    Arguments = cmdLine,
                    FileName = Assembly.GetExecutingAssembly().Location,
                    Verb = "runas",
                }).WaitForExit();
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
            if (args.Length > 1 || args[0].StartsWith("/") == false)
                return "help";
            return args[0].Substring(1);
        }

        private static void RunInDebugMode()
        {
            DivanServer.EnsureCanListenToWhenInNonAdminContext(8080);
            using (new DivanServer(new RavenConfiguration()))
            {
                Console.WriteLine("Raven is ready to process requests.");
                Console.WriteLine("Press any key to stop the server");
                Console.ReadLine();
            }
        }

        private static void PrintUsage()
        {
            Console.WriteLine(@"
Raven DB
Document Database for the .Net Platform
----------------------------------------
Copyright (C) 2010 - Hibernating Rhinos
----------------------------------------
Command line ptions:
    raven             - with no args, starts Raven in local server mode
    raven /install    - installs and starts the Raven service
    raven /unisntall  - stops and uninstalls the Raven service

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

                ManagedInstallerClass.InstallHelper(new string[] { "/u", Assembly.GetExecutingAssembly().Location });
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