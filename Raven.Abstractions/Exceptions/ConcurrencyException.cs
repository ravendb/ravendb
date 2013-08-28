//-----------------------------------------------------------------------
// <copyright file="ConcurrencyException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Exceptions
{
	/// <summary>
	/// This exception is raised when a concurrency conflict is encountered
	/// </summary>
#if !SILVERLIGHT && !NETFX_CORE
	[Serializable]
#endif
	public class ConcurrencyException : Exception
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
		/// </summary>
		public ConcurrencyException()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		public ConcurrencyException(string message) : base(message)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="inner">The inner.</param>
		public ConcurrencyException(string message, Exception inner) : base(message, inner)
		{
		}

#if !SILVERLIGHT && !NETFX_CORE
		/// <summary>
		/// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
		/// </summary>
		/// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
		/// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
		/// <exception cref="T:System.ArgumentNullException">The <paramref name="info"/> parameter is null. </exception>
		/// <exception cref="T:System.Runtime.Serialization.SerializationException">The class name is null or <see cref="P:System.Exception.HResult"/> is zero (0). </exception>
		protected ConcurrencyException(
			System.Runtime.Serialization.SerializationInfo info,
			System.Runtime.Serialization.StreamingContext context)
			: base(info, context)
		{
		}
#endif

		/// <summary>
		/// Gets or sets the expected E tag.
		/// </summary>
		/// <value>The expected E tag.</value>
		public Etag ExpectedETag { get; set; }
		/// <summary>
		/// Gets or sets the actual E tag.
		/// </summary>
		/// <value>The actual E tag.</value>
		public Etag ActualETag { get; set; }
	}
}
