//-----------------------------------------------------------------------
// <copyright file="Tracer.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System.Diagnostics;
using System.Text;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// A tracer object. This creates a trace switch which controls tracing.
    /// </summary>
    internal class Tracer
    {
        // Trace switches can be enabled in the Application.exe.config file. A sample
        // file might look like this:
        //
        //    <configuration>
        //        <system.diagnostics>
        //            <switches>
        //                <add name="Connection" value="4" />
        //                <add name="EseSql" value="4" />
        //                <add name="Table" value="4" />
        //            </switches>
        //        </system.diagnostics>
        //    </configuration>

        /// <summary>
        /// Controls the tracing of the object.
        /// </summary>
        private readonly TraceSwitch traceSwitch;

        /// <summary>
        /// String to be prefixed to all traces.
        /// </summary>
        private readonly string prefix;

        /// <summary>
        /// Initializes a new instance of the Tracer class.
        /// </summary>
        /// <param name="displayName">Name of the trace switch.</param>
        /// <param name="description">Description of the trace switch.</param>
        /// <param name="prefix">String to be prefixed to all traces for this object.</param>
        public Tracer(string displayName, string description, string prefix)
        {
            this.traceSwitch = new TraceSwitch(displayName, description);
            this.prefix = prefix;
        }

        /// <summary>
        /// Gets the trace options for this class.
        /// </summary>
        public TraceSwitch TraceSwitch
        {
            get
            {
                return this.traceSwitch;
            }
        }

        /// <summary>
        /// Logs a verbose trace message.
        /// </summary>
        /// <param name="format">A composite format string.</param>
        /// <param name="args">An array containing zero or more objects to format.</param>
        [Conditional("TRACE")]
        public void TraceVerbose(string format, params object[] args)
        {
            if (this.traceSwitch.TraceVerbose)
            {
                this.TraceImpl(format, args);
            }
        }

        /// <summary>
        /// Logs an info trace message.
        /// </summary>
        /// <param name="format">A composite format string.</param>
        /// <param name="args">An array containing zero or more objects to format.</param>
        [Conditional("TRACE")]
        public void TraceInfo(string format, params object[] args)
        {
            if (this.traceSwitch.TraceInfo)
            {
                this.TraceImpl(format, args);
            }
        }

        /// <summary>
        /// Logs a warning trace message.
        /// </summary>
        /// <param name="format">A composite format string.</param>
        /// <param name="args">An array containing zero or more objects to format.</param>
        [Conditional("TRACE")]
        public void TraceWarning(string format, params object[] args)
        {
            if (this.traceSwitch.TraceWarning)
            {
                this.TraceImpl(format, args);
            }
        }

        /// <summary>
        /// Logs an error trace message.
        /// </summary>
        /// <param name="format">A composite format string.</param>
        /// <param name="args">An array containing zero or more objects to format.</param>
        [Conditional("TRACE")]
        public void TraceError(string format, params object[] args)
        {
            if (this.traceSwitch.TraceError)
            {
                this.TraceImpl(format, args);
            }
        }

        /// <summary>
        /// Trace the given data. Don't call this directly, use TraceVerbose, TraceError
        /// or TraceWarning.
        /// </summary>
        /// <param name="format">A composite format string.</param>
        /// <param name="args">An array containing zero or more objects to format.</param>
        [Conditional("TRACE")]
        private void TraceImpl(string format, params object[] args)
        {
            var sb = new StringBuilder();
            sb.Append(this.prefix);
            sb.Append(": ");
            sb.AppendFormat(format, args);
            System.Diagnostics.Trace.WriteLine(sb.ToString());
        }
    }
}