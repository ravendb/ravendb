using System;
using System.IO;
using System.Linq;
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
                if (args.Length == 1)
                {
                    switch (args[0])
                    {
                        case "/service":
                            RunAsService();
                            break;

                        case "/install":
                            InstallAndStart();
                            break;

                        case "/uninstall":
                            EnsureStoppedAndUninstall();
                            break;
                    }
                }
                else
                {
                    DivanServer.EnsureCanListenToWhenInNonAdminContext(8080);
                    Console.WriteLine(Path.GetFullPath(@"..\..\..\Data"));
                    using (new DivanServer(@"..\..\..\Data", 8080))
                    {
                        Console.WriteLine("Ready to process requests...");
                        Console.ReadLine();
                    }
                }
            }
            else
            {
                ServiceBase.Run(new RavenService());
            }
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
            if (ServiceIsInstalled() == true)
            {
                Console.WriteLine("Service is already installed");
            }
            else
            {
                ManagedInstallerClass.InstallHelper(new string[] { Assembly.GetExecutingAssembly().Location });
                var startController = new ServiceController(ProjectInstaller.SERVICE_NAME);
                startController.Start();
            }
        }

        private static void RunAsService()
        {
            //TODO: not sure about this one
        }

        private static bool ServiceIsInstalled()
        {
            return (ServiceController.GetServices().Count(s => s.ServiceName == ProjectInstaller.SERVICE_NAME) > 0);
        }
    }
}