//-----------------------------------------------------------------------
// <copyright file="EsentVersion.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using Microsoft.Isam.Esent.Interop.Implementation;

    /// <summary>
    /// Gives information about the version of esent being used.
    /// </summary>
    public static class EsentVersion
    {
        /// <summary>
        /// Gets a value indicating whether the current version of esent
        /// supports features available in the Windows Server 2003 version of
        /// esent.
        /// </summary>
        public static bool SupportsServer2003Features
        {
            get
            {
                return Capabilities.SupportsServer2003Features;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the current version of esent
        /// supports features available in the Windows Vista version of
        /// esent.
        /// </summary>
        public static bool SupportsVistaFeatures
        {
            get
            {
                return Capabilities.SupportsVistaFeatures;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the current version of esent
        /// supports features available in the Windows 7 version of
        /// esent.
        /// </summary>
        public static bool SupportsWindows7Features
        {
            get
            {
                return Capabilities.SupportsWindows7Features;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the current version of esent
        /// can use non-ASCII paths to access databases.
        /// </summary>
        public static bool SupportsUnicodePaths
        {
            get
            {
                return Capabilities.SupportsUnicodePaths;
            }
        }

        /// <summary>
        /// Gets a value indicating whether large (> 255 byte) keys are supported.
        /// The key size for an index can be specified in the <see cref="JET_INDEXCREATE"/>
        /// object.
        /// </summary>
        public static bool SupportsLargeKeys
        {
            get
            {
                return Capabilities.SupportsLargeKeys;
            }
        }

        /// <summary>
        /// Gets a description of the current Esent capabilities.
        /// </summary>
        /// <remarks>
        /// We allow this to be set separately so that capabilities can
        /// be downgraded for testing.
        /// </remarks>
        private static JetCapabilities Capabilities
        {
            get
            {
                return Api.Impl.Capabilities;
            }
        }
    }
}