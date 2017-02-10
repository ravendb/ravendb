//-----------------------------------------------------------------------
// <copyright file="DatabaseDoesNotExistException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Exceptions.Database
{
    public class DatabaseDisabledException : RavenException
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