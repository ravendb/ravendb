//-----------------------------------------------------------------------
// <copyright file="NonAuthoritativeInformationException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.Client.Exceptions
{
	/// <summary>
	/// This exception is raised when a non authoritative information is encountered
	/// </summary>
#if !SILVERLIGHT
	[Serializable]
#endif
	public class NonAuthoritativeInformationException : Exception
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="NonAuthoritativeInformationException"/> class.
		/// </summary>
		public NonAuthoritativeInformationException()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="NonAuthoritativeInformationException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		public NonAuthoritativeInformationException(string message) : base(message)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="NonAuthoritativeInformationException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="inner">The inner.</param>
		public NonAuthoritativeInformationException(string message, Exception inner) : base(message, inner)
		{
		}

#if !SILVERLIGHT
		/// <summary>
		/// Initializes a new instance of the <see cref="NonAuthoritativeInformationException"/> class.
		/// </summary>
		/// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
		/// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
		/// <exception cref="T:System.ArgumentNullException">The <paramref name="info"/> parameter is null. </exception>
		/// <exception cref="T:System.Runtime.Serialization.SerializationException">The class name is null or <see cref="P:System.Exception.HResult"/> is zero (0). </exception>
		protected NonAuthoritativeInformationException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
#endif
	}
}
