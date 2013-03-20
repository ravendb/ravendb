// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionaryTracing.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   PersistentDictionary tracing code.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Diagnostics;
    using System.Globalization;

    /// <content>
    /// PersistentDictionary tracing.
    /// </content>
    public partial class PersistentDictionary<TKey, TValue> where TKey : IComparable<TKey>
    {
        /// <summary>
        /// PersistentDictionary tracing.
        /// </summary>
        private readonly TraceSwitch traceSwitch = new TraceSwitch("PersistentDictionary", "PersistentDictionary");

        /// <summary>
        /// Gets the TraceSwitch for the dictionary.
        /// </summary>
        /// <value>
        /// The TraceSwitch for the dictionary.
        /// </value>
        public TraceSwitch TraceSwitch
        {
            [DebuggerStepThrough]
            get { return this.traceSwitch; }
        }

        /// <summary>
        /// Trace the results of examining a Where expression.
        /// </summary>
        /// <param name="range">The calculated range.</param>
        /// <param name="isReversed">True if the range is to be enumerated in reverse order.</param>
        [Conditional("TRACE")]
        internal void TraceWhere(KeyRange<TKey> range, bool isReversed)
        {
            Trace.WriteLineIf(this.traceSwitch.TraceVerbose, String.Format(CultureInfo.InvariantCulture, "WHERE: {0} {1}", range, isReversed ? "(reversed)" : String.Empty));
        }
    }
}
