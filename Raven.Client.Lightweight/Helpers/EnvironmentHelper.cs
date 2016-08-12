using System;
using System.Net;

namespace Raven.Client.Helpers
{
    internal static class EnvironmentHelper
    {
        public static bool Is64BitProcess
        {
            get
            {
                return Environment.Is64BitProcess;
            }
        }

        public static ulong AvailablePhysicalMemory
        {
            get
            {
                return new Microsoft.VisualBasic.Devices.ComputerInfo().AvailablePhysicalMemory;
            }
        }

        public static string MachineName
        {
            get
            {
                return Environment.MachineName;
            }
        }
    }
}