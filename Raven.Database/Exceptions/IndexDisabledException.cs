//-----------------------------------------------------------------------
// <copyright file="IndexDisabledException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;
using Raven.Database.Data;

namespace Raven.Database.Exceptions
{
	/// <summary>
	/// This exception is raised when querying an index that was disabled because the error rate exceeded safety margins
	/// </summary>
	[Serializable]
	public class IndexDisabledException : Exception
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDisabledException"/> class.
		/// </summary>
		public IndexDisabledException()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDisabledException"/> class.
		/// </summary>
		/// <param name="information">The information.</param>
		public IndexDisabledException(IndexFailureInformation information)
		{
			Information = information;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDisabledException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		public IndexDisabledException(string message) : base(message)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDisabledException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="inner">The inner.</param>
		public IndexDisabledException(string message, Exception inner) : base(message, inner)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDisabledException"/> class.
		/// </summary>
		/// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
		/// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
		/// <exception cref="T:System.ArgumentNullException">The <paramref name="info"/> parameter is null. </exception>
		/// <exception cref="T:System.Runtime.Serialization.SerializationException">The class name is null or <see cref="P:System.Exception.HResult"/> is zero (0). </exception>
		protected IndexDisabledException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}

		/// <summary>
		/// Gets or sets the information about the index failure 
		/// </summary>
		/// <value>The information.</value>
		public IndexFailureInformation Information { get; set; }
	}
}
