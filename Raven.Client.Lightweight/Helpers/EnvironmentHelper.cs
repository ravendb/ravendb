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
#if !DNXCORE50
                return Environment.Is64BitProcess;
#else
                return IntPtr.Size == 8;
#endif
            }
        }

        public static ulong AvailablePhysicalMemory
        {
            get
            {
#if !DNXCORE50
                return new Microsoft.VisualBasic.Devices.ComputerInfo().AvailablePhysicalMemory;
#else
                // TODO [ppekrol] how to check this?
                throw new NotImplementedException();
#endif
            }
        }

        public static string MachineName
        {
            get
            {
#if !DNXCORE50
                return Environment.MachineName;
#else
                return Dns.GetHostName();
#endif
            }
        }
    }
}