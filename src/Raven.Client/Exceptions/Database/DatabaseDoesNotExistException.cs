//-----------------------------------------------------------------------
// <copyright file="DatabaseDoesNotExistsException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.NewClient.Client.Exceptions.Database
{
    public class DatabaseDoesNotExistException : RavenException
    {
        public DatabaseDoesNotExistException()
        {
        }

        public DatabaseDoesNotExistException(string msg) : base(msg)
        {

        }

        public DatabaseDoesNotExistException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}