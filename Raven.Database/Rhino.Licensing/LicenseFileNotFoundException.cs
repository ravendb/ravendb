using System;
using System.Runtime.Serialization;

namespace Rhino.Licensing
{
    /// <summary>
    /// Thrown when a valid license file can not be
    /// found on the client machine.
    /// </summary>
    [Serializable]
    public class LicenseFileNotFoundException : RhinoLicensingException
    {
        /// <summary>
        /// Creates a new instance of <seealso cref="LicenseFileNotFoundException"/>
        /// </summary>
        public LicenseFileNotFoundException()
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="LicenseFileNotFoundException"/>
        /// </summary>
        /// <param name="message">error message</param>
        public LicenseFileNotFoundException(string message) 
            : base(message)
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="LicenseFileNotFoundException"/>
        /// </summary>
        /// <param name="message">error message</param>
        /// <param name="inner">inner exception</param>
        public LicenseFileNotFoundException(string message, Exception inner) 
            : base(message, inner)
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="LicenseFileNotFoundException"/>
        /// </summary>
        /// <param name="info">serialization information</param>
        /// <param name="context">streaming context</param>
        protected LicenseFileNotFoundException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}