using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using Sparrow.Platform;

namespace Raven.Client.Extensions
{
     public static class WhoIsLocking
    {
        private const int RmRebootReasonNone = 0;
        private const int CCH_RM_MAX_APP_NAME = 255;
        private const int CCH_RM_MAX_SVC_NAME = 63;

        public static string ThisFile(string path)
        {
            var processesUsingFiles = GetProcessesUsingFile(path);
            var stringBuilder = new StringBuilder();
            stringBuilder.Append("The following processes are locking ").Append(path).AppendLine();
            foreach (var processesUsingFile in processesUsingFiles)
            {
                stringBuilder.Append("\t").Append(processesUsingFile.ProcessName).Append(' ').Append(processesUsingFile.Id).
                    AppendLine();
            }
            return stringBuilder.ToString();
        }

        public static IList<Process> GetProcessesUsingFile(string filePath)
        {
            var processes = new List<Process>();
            if (PlatformDetails.RunningOnPosix)
                return processes;

            // Create a restart manager session
            int rv;
            uint sessionHandle;
            try
            {
                rv = RmStartSession(out sessionHandle, 0, Guid.NewGuid().ToString());
            }
            catch (DllNotFoundException)
            {
                return processes;
            }
            if (rv != 0)
                throw new Win32Exception(Marshal.GetLastWin32Error(), $"Failed to RmStartSession (error: {rv})");
            try
            {
                // Let the restart manager know what files we’re interested in
                var pathStrings = new[]{filePath};
                rv = RmRegisterResources(sessionHandle,
                                         (uint) pathStrings.Length, pathStrings,
                                         0, null, 0, null);
                if (rv != 0)
                    throw new Win32Exception(Marshal.GetLastWin32Error(), $"Failed to RmRegisterResources for file '{filePath}' with error {rv} (sessionHandle={sessionHandle})");

                // Ask the restart manager what other applications 
                // are using those files
                const int ERROR_MORE_DATA = 234;
                uint pnProcInfo = 0,
                     lpdwRebootReasons = RmRebootReasonNone;
                rv = RmGetList(sessionHandle, out uint pnProcInfoNeeded,
                               ref pnProcInfo, null, ref lpdwRebootReasons);
                if (rv == ERROR_MORE_DATA)
                {
                    // Create an array to store the process results
                    var processInfo = new RM_PROCESS_INFO[pnProcInfoNeeded];
                    pnProcInfo = (uint) processInfo.Length;

                    // Get the list
                    rv = RmGetList(sessionHandle, out pnProcInfoNeeded,
                                   ref pnProcInfo, processInfo, ref lpdwRebootReasons);
                    if (rv == 0)
                    {
                        // Enumerate all of the results and add them to the 
                        // list to be returned
                        for (int i = 0; i < pnProcInfo; i++)
                        {
                            try
                            {
                                processes.Add(Process.GetProcessById(processInfo[i].Process.dwProcessId));
                            }
                            catch (ArgumentException)
                            {
                                // in case the process is no longer running
                            }
                        }
                    }
                    else
                        throw new Win32Exception(Marshal.GetLastWin32Error(), $"Failed to RmGetList for file '{filePath}' with error {rv} (sessionHandle={sessionHandle})");
                }
                else if (rv != 0)
                    throw new Win32Exception(Marshal.GetLastWin32Error(), $"Failed to RmGetList for file '{filePath}' with error {rv} (sessionHandle={sessionHandle})");
            }
            finally
            {
                // Close the resource manager
                RmEndSession(sessionHandle);
            }

            return processes;
        }

        [DllImport("rstrtmgr.dll", CharSet = CharSet.Unicode)]
        private static extern int RmStartSession(
            out uint pSessionHandle, int dwSessionFlags, string strSessionKey);

        [DllImport("rstrtmgr.dll")]
        private static extern int RmEndSession(uint pSessionHandle);

        [DllImport("rstrtmgr.dll", CharSet = CharSet.Unicode)]
        private static extern int RmRegisterResources(uint pSessionHandle,
                                                      UInt32 nFiles, string[] rgsFilenames,
                                                      UInt32 nApplications, [In] RM_UNIQUE_PROCESS[] rgApplications,
                                                      UInt32 nServices, string[] rgsServiceNames);

        [DllImport("rstrtmgr.dll")]
        private static extern int RmGetList(uint dwSessionHandle,
                                            out uint pnProcInfoNeeded, ref uint pnProcInfo,
                                            [In, Out] RM_PROCESS_INFO[] rgAffectedApps,
                                            ref uint lpdwRebootReasons);

        #region Nested type: RM_APP_TYPE

        private enum RM_APP_TYPE
        {
            RmUnknownApp = 0,
            RmMainWindow = 1,
            RmOtherWindow = 2,
            RmService = 3,
            RmExplorer = 4,
            RmConsole = 5,
            RmCritical = 1000
        }

        #endregion

        #region Nested type: RM_PROCESS_INFO

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct RM_PROCESS_INFO
        {
            public RM_UNIQUE_PROCESS Process;
            [MarshalAs(UnmanagedType.ByValTStr,
                SizeConst = CCH_RM_MAX_APP_NAME + 1)] public readonly string strAppName;
            [MarshalAs(UnmanagedType.ByValTStr,
                SizeConst = CCH_RM_MAX_SVC_NAME + 1)] public readonly string strServiceShortName;
            public readonly RM_APP_TYPE ApplicationType;
            public readonly uint AppStatus;
            public readonly uint TSSessionId;
            [MarshalAs(UnmanagedType.Bool)] public readonly bool bRestartable;
        }

        #endregion

        #region Nested type: RM_UNIQUE_PROCESS

        [StructLayout(LayoutKind.Sequential)]
        private struct RM_UNIQUE_PROCESS
        {
            public readonly int dwProcessId;
            public readonly FILETIME ProcessStartTime;
        }

        #endregion
    }
}
