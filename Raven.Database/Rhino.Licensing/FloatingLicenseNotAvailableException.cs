using System;
using System.Runtime.Serialization;

namespace Rhino.Licensing
{
	/// <summary>
	/// 
	/// </summary>
	public class FloatingLicenseNotAvailableException : RhinoLicensingException
	{
		/// <summary>
		/// Creates a new instance of <seealso cref="FloatingLicenseNotAvailableException"/>.
		/// </summary>
		public FloatingLicenseNotAvailableException()
		{
		}

		/// <summary>
		/// Creates a new instance of <seealso cref="FloatingLicenseNotAvailableException"/>.
		/// </summary>
		/// <param name="message">error message</param>
		public FloatingLicenseNotAvailableException(string message)
			: base(message)
		{
		}

		/// <summary>
		/// Creates a new instance of <seealso cref="FloatingLicenseNotAvailableException"/>.
		/// </summary>
		/// <param name="message">error message</param>
		/// <param name="inner">inner exception</param>
		public FloatingLicenseNotAvailableException(string message, Exception inner)
			: base(message, inner)
		{
		}

		/// <summary>
		/// Creates a new instance of <seealso cref="FloatingLicenseNotAvailableException"/>.
		/// </summary>
		/// <param name="info">serialization information</param>
		/// <param name="context">streaming context</param>
		protected FloatingLicenseNotAvailableException(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context)
		{
		}
	}
}