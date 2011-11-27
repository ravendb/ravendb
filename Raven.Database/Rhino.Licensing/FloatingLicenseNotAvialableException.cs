using System;
using System.Runtime.Serialization;

namespace Rhino.Licensing
{
    /// <summary>
    /// 
    /// </summary>
    public class FloatingLicenseNotAvialableException : RhinoLicensingException
    {
        /// <summary>
        /// Creates a new instance of <seealso cref="FloatingLicenseNotAvialableException"/>.
        /// </summary>
        public FloatingLicenseNotAvialableException()
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="FloatingLicenseNotAvialableException"/>.
        /// </summary>
        /// <param name="message">error message</param>
        public FloatingLicenseNotAvialableException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="FloatingLicenseNotAvialableException"/>.
        /// </summary>
        /// <param name="message">error message</param>
        /// <param name="inner">inner exception</param>
        public FloatingLicenseNotAvialableException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="FloatingLicenseNotAvialableException"/>.
        /// </summary>
        /// <param name="info">serialization information</param>
        /// <param name="context">streaming context</param>
        protected FloatingLicenseNotAvialableException(
            SerializationInfo info,
            StreamingContext context)
            : base(info, context)
        {
        }
    }
}