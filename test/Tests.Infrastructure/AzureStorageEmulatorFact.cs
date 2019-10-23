using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using FastTests;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Utils;
using Sparrow.Platform;
using Xunit;

namespace Tests.Infrastructure
{
    public class AzureStorageEmulatorFact : FactAttribute
    {
        private const string AzureStorageEmulatorPath = "C:\\Program Files (x86)\\Microsoft SDKs\\Azure\\Storage Emulator\\AzureStorageEmulator.exe";

        static AzureStorageEmulatorFact()
        {
            RavenAzureClient.TestMode = true;

            if (RavenTestHelper.IsRunningOnCI == false)
                return;

            if (PlatformDetails.RunningOnPosix)
                return;

            if (File.Exists(AzureStorageEmulatorPath) == false)
                return;

            KillAzureStorageEmulator();
            InitializeAzureStorageEmulator();
        }
         
        public AzureStorageEmulatorFact([CallerMemberName] string memberName = "")
        {
            if (PlatformDetails.RunningOnPosix)
            {
                Skip = "Azure Storage Emulator tests can only run on Windows";
                return;
            }

            if (File.Exists(AzureStorageEmulatorPath) == false)
            {
                Skip = "Azure Storage Emulator is not installed";
                return;
            }

            var process = Process.GetProcessesByName("AzureStorageEmulator").FirstOrDefault();
            if (process == null)
            {
                Skip = $"Cannot execute '{memberName}' because Azure Storage Emulator isn't running";
                return;
            }
        }

        private static void InitializeAzureStorageEmulator()
        {
            // clear the storage
            var process = new Process
            {
                StartInfo =
                {
                    FileName = AzureStorageEmulatorPath,
                    Arguments = "clear"
                }
            };

            process.Start();
            process.WaitForExit();

            foreach (var file in Directory.GetFileSystemEntries(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "AzureStorageEmulatorDb*.*"))
                IOExtensions.DeleteFile(file);

            process = new Process
            {
                StartInfo =
                {
                    FileName = AzureStorageEmulatorPath,
                    Arguments = "init /forceCreate"
                }
            };

            process.Start();
            process.WaitForExit();

            process = new Process
            {
                StartInfo =
                {
                    FileName = AzureStorageEmulatorPath,
                    Arguments = "start"
                }
            };

            process.Start();
            process.WaitForExit();
        }

        private static void KillAzureStorageEmulator()
        {
            foreach (var process in Process.GetProcessesByName("csmonitor"))
            {
                try
                {
                    process.Kill();
                }
                catch
                {
                }
            }

            foreach (var process in Process.GetProcessesByName("AzureStorageEmulator"))
            {
                try
                {
                    process.Kill();
                }
                catch
                {
                }
            }
        }
    }
}
