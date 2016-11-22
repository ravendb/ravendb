// -----------------------------------------------------------------------
//  <copyright file="ImplicitFetchFieldsFromDocumentNotAllowedException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.NewClient.Abstractions.Exceptions
{
    public class ImplicitFetchFieldsFromDocumentNotAllowedException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ImplicitFetchFieldsFromDocumentNotAllowedException"/> class.
        /// </summary>
        public ImplicitFetchFieldsFromDocumentNotAllowedException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ImplicitFetchFieldsFromDocumentNotAllowedException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public ImplicitFetchFieldsFromDocumentNotAllowedException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ImplicitFetchFieldsFromDocumentNotAllowedException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public ImplicitFetchFieldsFromDocumentNotAllowedException(string message, Exception inner) : base(message, inner)
        {
        }

    }
}
