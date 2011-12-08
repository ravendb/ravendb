using System;
using System.Runtime.Serialization;

namespace Rhino.Licensing
{
	[Serializable]
	public class CorruptLicenseFileException : RhinoLicensingException
	{
		/// <summary>
		/// Creates a new instance of <seealso cref="RhinoLicensingException"/>.
		/// </summary>
		public CorruptLicenseFileException()
		{
		}

		/// <summary>
		/// Creates a new instance of <seealso cref="RhinoLicensingException"/>.
		/// </summary>
		/// <param name="message">error message</param>
		public CorruptLicenseFileException(string message)
			: base(message)
		{
		}

		/// <summary>
		/// Creates a new instance of <seealso cref="RhinoLicensingException"/>.
		/// </summary>
		/// <param name="message">error message</param>
		/// <param name="inner">inner exception</param>
		public CorruptLicenseFileException(string message, Exception inner)
			: base(message, inner)
		{
		}

		/// <summary>
		/// Creates a new instance of <seealso cref="RhinoLicensingException"/>.
		/// </summary>
		/// <param name="info">serialization information</param>
		/// <param name="context">streaming context</param>
		protected CorruptLicenseFileException(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context)
		{
		}
	}
}