// --------------------------------------------------------------------------------------------------------------------
// <copyright file="CollectionsSystemParameters.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Code to set global ESENT parameters. These are the parameters that have to
//   be set before the first instance is created.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Global parameters for all collections.
    /// </summary>
    internal static class Globals
    {
        /// <summary>
        /// Used to make sure only one thread can perform the global initialization.
        /// </summary>
        private static readonly object initLock = new object();

        /// <summary>
        /// True if the Init() method has already run.
        /// </summary>
        private static bool isInit;

        /// <summary>
        /// A global initialization function. This should be called
        /// exactly once, before any ESENT instances are created.
        /// </summary>
        public static void Init()
        {
            if (!isInit)
            {
                lock (initLock)
                {
                    if (!isInit)
                    {
                        DoInit();
                        isInit = true;
                    }
                }
            }
        }

        /// <summary>
        /// Perform the global initialization. This sets the page
        /// size, configuration, cache size and other global
        /// parameters.
        /// </summary>
        private static void DoInit()
        {
            SystemParameters.Configuration = 1;
            SystemParameters.EnableAdvanced = true;
            SystemParameters.MaxInstances = 1024;
            SystemParameters.DatabasePageSize = 8192;
            SystemParameters.CacheSizeMin = 8192; // 64MB
            SystemParameters.CacheSizeMax = Int32.MaxValue;
        }
    }
}

