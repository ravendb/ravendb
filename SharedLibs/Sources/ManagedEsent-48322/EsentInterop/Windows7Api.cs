//-----------------------------------------------------------------------
// <copyright file="Windows7Api.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Windows7
{
    /// <summary>
    /// ESENT APIs that were first supported in Windows 7 (Windows Server 2008 R2).
    /// </summary>
    public static class Windows7Api
    {
        /// <summary>
        /// Crash dump options for Watson.
        /// </summary>
        /// <param name="grbit">Crash dump options.</param>
        public static void JetConfigureProcessForCrashDump(CrashDumpGrbit grbit)
        {
            Api.Check(Api.Impl.JetConfigureProcessForCrashDump(grbit));
        }
    }
}