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

        public DatabaseDoesNotExistsException(string message)
            : base(message)
        {
        }
    }
}