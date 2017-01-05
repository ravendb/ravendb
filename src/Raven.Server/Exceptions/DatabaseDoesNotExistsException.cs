//-----------------------------------------------------------------------
// <copyright file="DatabaseDoesNotExistsException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Server.Exceptions
{
    public class DatabaseDoesNotExistsException : Exception
    {
        public DatabaseDoesNotExistsException()
        {
        }

        public DatabaseDoesNotExistsException(string msg) : base(msg)
        {

        }

        public DatabaseDoesNotExistsException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}