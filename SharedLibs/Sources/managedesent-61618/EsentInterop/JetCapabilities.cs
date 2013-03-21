//-----------------------------------------------------------------------
// <copyright file="JetCapabilities.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Implementation
{
    /// <summary>
    /// Describes the functionality exposed by an object which implements IJetApi.
    /// </summary>
    internal sealed class JetCapabilities
    {
        /// <summary>
        /// Gets or sets a value indicating whether Windows Server 2003 features
        /// (in the Interop.Server2003 namespace) are supported.
        /// </summary>
        public bool SupportsServer2003Features { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether Vista features (in the
        /// Interop.Vista namespace) are supported.
        /// </summary>
        public bool SupportsVistaFeatures { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether Win7 features (in the
        /// Interop.Windows7 namespace) are supported.
        /// </summary>
        public bool SupportsWindows7Features { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether unicode file paths are supported.
        /// </summary>
        public bool SupportsUnicodePaths { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether large (> 255 byte) keys are supported.
        /// The key size for an index can be specified in the <see cref="JET_INDEXCREATE"/>
        /// object.
        /// </summary>
        public bool SupportsLargeKeys { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of components in a sort or index key.
        /// </summary>
        public int ColumnsKeyMost { get; set; }
    }
}