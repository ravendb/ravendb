//-----------------------------------------------------------------------
// <copyright file="OperationVetoedException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions
{
	/// <summary>
	/// This exception is raised when an operation has been vetoed by a trigger
    /// </summary>
#if !NETFX_CORE
    [Serializable]
#endif

	public class OperationVetoedException : Exception
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="OperationVetoedException"/> class.
		/// </summary>
		public OperationVetoedException()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OperationVetoedException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		public OperationVetoedException(string message) : base(message)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OperationVetoedException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="inner">The inner.</param>
		public OperationVetoedException(string message, Exception inner) : base(message, inner)
		{
		}

#if !NETFX_CORE

        /// <summary>
	/// Initializes a new instance of the <see cref="OperationVetoedException"/> class.
	/// </summary>
	/// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
	/// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
	/// <exception cref="T:System.ArgumentNullException">The <paramref name="info"/> parameter is null. </exception>
	/// <exception cref="T:System.Runtime.Serialization.SerializationException">The class name is null or <see cref="P:System.Exception.HResult"/> is zero (0). </exception>
		protected OperationVetoedException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
#endif

	}
}
