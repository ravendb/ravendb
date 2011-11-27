using System;
using System.Runtime.Serialization;

namespace Rhino.Licensing
{
    /// <summary>
    /// Base class for all licensing exceptions.
    /// </summary>
    [Serializable]
    public class RhinoLicensingException : Exception
    {
        /// <summary>
        /// Creates a new instance of <seealso cref="RhinoLicensingException"/>.
        /// </summary>
        protected RhinoLicensingException()
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="RhinoLicensingException"/>.
        /// </summary>
        /// <param name="message">error message</param>
        protected RhinoLicensingException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="RhinoLicensingException"/>.
        /// </summary>
        /// <param name="message">error message</param>
        /// <param name="inner">inner exception</param>
        protected RhinoLicensingException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Creates a new instance of <seealso cref="RhinoLicensingException"/>.
        /// </summary>
        /// <param name="info">serialization information</param>
        /// <param name="context">streaming context</param>
        protected RhinoLicensingException(
            SerializationInfo info,
            StreamingContext context)
            : base(info, context)
        {
        }
    }
}