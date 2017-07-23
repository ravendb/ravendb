using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Xunit;

namespace Tests.Infrastructure
{
    [CLSCompliant(false)]
    public class AzureStorageEmulatorRunning : FactAttribute
    {
        public AzureStorageEmulatorRunning([CallerMemberName] string memberName = "")
        {
            var process = Process.GetProcessesByName("AzureStorageEmulator").FirstOrDefault();
            if (process != null)
                return;

            Skip = $"Cannot execute {memberName} because Azure Storage Emulator isn't running";
        }
    }
}