//-----------------------------------------------------------------------
// <copyright file="DatabaseDoesNotExistsException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Server.Exceptions
{
    public class DatabaseDisabledException : Exception
    {
        public DatabaseDisabledException()
        {
        }

        public DatabaseDisabledException(string message)
            : base(message)
        {
        }
    }
}