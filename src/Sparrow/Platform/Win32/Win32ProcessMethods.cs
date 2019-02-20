using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Sparrow.Platform.Win32
{
    public class Win32ProcessMethods
    {
        public static IntPtr CurrentProcess = GetCurrentProcess();

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern IntPtr GetCurrentProcess();
    }
}
