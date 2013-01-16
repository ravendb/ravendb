//-----------------------------------------------------------------------
// <copyright file="JetParameter.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent.Interop;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// This class contains one Esent parameter setting.
    /// </summary>
    internal class JetParameter
    {
        /// <summary>
        /// The trace object for this class.
        /// </summary>
        private static readonly Tracer tracer = new Tracer("JetParameter", "Parameter settings for Esent", "JetParameter");

        /// <summary>
        /// The paramemter identifier.
        /// </summary>
        private readonly JET_param param;

        /// <summary>
        /// The string value of the parameter (may be null)
        /// </summary>
        private readonly string strValue;

        /// <summary>
        /// The integer value of the parameter (may be null).
        /// </summary>
        private readonly int intValue;

        /// <summary>
        /// Initializes a new instance of the JetParameter class.
        /// </summary>
        /// <param name="param">The parameter identifier.</param>
        /// <param name="value">The value of the parameter.</param>
        public JetParameter(JET_param param, int value)
        {
            this.param = param;
            this.strValue = null;
            this.intValue = value;
        }

        /// <summary>
        /// Initializes a new instance of the JetParameter class.
        /// </summary>
        /// <param name="param">The parameter identifier.</param>
        /// <param name="value">The value of the parameter.</param>
        public JetParameter(JET_param param, string value)
        {
            this.param = param;
            this.strValue = value;
            this.intValue = 0;
        }

        /// <summary>
        /// Gets the Tracer for this object.
        /// </summary>
        private Tracer Tracer
        {
            get
            {
                return tracer;
            }
        }

        /// <summary>
        /// Get the string representation of this parameter setting.
        /// </summary>
        /// <returns>A string representation of the parameter.</returns>
        public override string ToString()
        {
            return string.Format("{0} ({1}/{2})", this.param, this.intValue, this.strValue);
        }

        /// <summary>
        /// Sets this parameter on the specified instance.
        /// </summary>
        public void SetParameter()
        {
            this.Tracer.TraceInfo("Setting {0} globally", this.ToString());
            Api.JetSetSystemParameter(JET_INSTANCE.Nil, JET_SESID.Nil, this.param, this.intValue, this.strValue);
        }

        /// <summary>
        /// Sets this parameter on the specified instance.
        /// </summary>
        /// <param name="instance">The instance to set the parameter on.</param>
        public void SetParameter(Instance instance)
        {
            this.Tracer.TraceInfo("Setting {0} on instance {1}", this.ToString(), instance);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, this.param, this.intValue, this.strValue);
        }
    }
}