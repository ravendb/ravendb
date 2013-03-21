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

        /// <summary>
        /// If the records with the specified keys are not in the buffer cache
        /// then start asynchronous reads to bring the records into the database
        /// buffer cache.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to issue the prereads against.</param>
        /// <param name="keys">
        /// The keys to preread. The keys must be sorted.
        /// </param>
        /// <param name="keyLengths">The lengths of the keys to preread.</param>
        /// <param name="keyIndex">
        /// The index of the first key in the keys array to read.
        /// </param>
        /// <param name="keyCount">
        /// The maximum number of keys to preread.
        /// </param>
        /// <param name="keysPreread">
        /// Returns the number of keys to actually preread.
        /// </param>
        /// <param name="grbit">
        /// Preread options. Used to specify the direction of the preread.
        /// </param>
        public static void JetPrereadKeys(
            JET_SESID sesid,
            JET_TABLEID tableid,
            byte[][] keys,
            int[] keyLengths,
            int keyIndex,
            int keyCount,
            out int keysPreread,
            PrereadKeysGrbit grbit)
        {
            Api.Check(Api.Impl.JetPrereadKeys(sesid, tableid, keys, keyLengths, keyIndex, keyCount, out keysPreread, grbit));
        }

        /// <summary>
        /// If the records with the specified keys are not in the buffer cache
        /// then start asynchronous reads to bring the records into the database
        /// buffer cache.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to issue the prereads against.</param>
        /// <param name="keys">
        /// The keys to preread. The keys must be sorted.
        /// </param>
        /// <param name="keyLengths">The lengths of the keys to preread.</param>
        /// <param name="keyCount">
        /// The maximum number of keys to preread.
        /// </param>
        /// <param name="keysPreread">
        /// Returns the number of keys to actually preread.
        /// </param>
        /// <param name="grbit">
        /// Preread options. Used to specify the direction of the preread.
        /// </param>
        public static void JetPrereadKeys(
            JET_SESID sesid,
            JET_TABLEID tableid,
            byte[][] keys,
            int[] keyLengths,
            int keyCount,
            out int keysPreread,
            PrereadKeysGrbit grbit)
        {
            JetPrereadKeys(sesid, tableid, keys, keyLengths, 0, keyCount, out keysPreread, grbit);
        }
    }
}