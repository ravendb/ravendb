//-----------------------------------------------------------------------
// <copyright file="ConcurrencyException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions.Routing
{
    /// <summary>
    /// This exception is raised when a request is created to a route that was not found on the server.
    /// </summary>
    public class RouteNotFoundException : RavenException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RouteNotFoundException"/> class.
        /// </summary>
        public RouteNotFoundException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RouteNotFoundException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public RouteNotFoundException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RouteNotFoundException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public RouteNotFoundException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
