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
                return IntPtr.Size == 8;
            }
        }

        public static ulong AvailablePhysicalMemory
        {
            get
            {
                throw new NotImplementedException("Probably need to call native API here?");
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