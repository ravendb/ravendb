using System;
using System.Runtime.Serialization;

namespace Rhino.Licensing
{
    /// <summary>
    /// Thrown when suitable license is not found.
    /// </summary>
    [Serializable]
    public class LicenseNotFoundException : RhinoLicensingException
    {
        /// <summary>
        /// Creates a new instance of <seealso cref="LicenseNotFoundException"/>.
        /// </summary>
        public LicenseNotFoundException()
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="LicenseNotFoundException"/>.
        /// </summary>
        /// <param name="message">error message</param>
        public LicenseNotFoundException(string message) 
            : base(message)
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="LicenseNotFoundException"/>.
        /// </summary>
        /// <param name="message">error message</param>
        /// <param name="inner">inner exception</param>
        public LicenseNotFoundException(string message, Exception inner) 
            : base(message, inner)
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="LicenseNotFoundException"/>.
        /// </summary>
        /// <param name="info">serialization information</param>
        /// <param name="context">steaming context</param>
        protected LicenseNotFoundException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}