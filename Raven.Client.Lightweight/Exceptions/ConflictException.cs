//-----------------------------------------------------------------------
// <copyright file="ConflictException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.Client.Exceptions
{
	/// <summary>
	/// This exception occurs when a (replication) conflict is encountered.
	/// Usually this required a user to manually resolve the conflict.
	/// </summary>
#if !SILVERLIGHT
	[Serializable]
#endif
	public class ConflictException : Exception
	{
		/// <summary>
		/// Gets or sets the conflicted version ids.
		/// </summary>
		/// <value>The conflicted version ids.</value>
		public string[] ConflictedVersionIds { get; set; }


		/// <summary>
		/// Gets or sets the conflicted document etag
		/// </summary>
		public Guid Etag { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="ConflictException"/> class.
		/// </summary>
		public ConflictException()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ConflictException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		public ConflictException(string message)
			: base(message)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ConflictException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="inner">The inner.</param>
		public ConflictException(string message, Exception inner)
			: base(message, inner)
		{
		}

#if !SILVERLIGHT
		/// <summary>
		/// Initializes a new instance of the <see cref="ConflictException"/> class.
		/// </summary>
		/// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
		/// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
		/// <exception cref="T:System.ArgumentNullException">The <paramref name="info"/> parameter is null. </exception>
		/// <exception cref="T:System.Runtime.Serialization.SerializationException">The class name is null or <see cref="P:System.Exception.HResult"/> is zero (0). </exception>
		protected ConflictException(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context)
		{
		}
#endif
	}
}
