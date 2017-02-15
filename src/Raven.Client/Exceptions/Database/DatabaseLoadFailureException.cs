//-----------------------------------------------------------------------
// <copyright file="DatabaseLoadFailureException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions.Database
{
    public class DatabaseLoadFailureException : RavenException
    {
        public DatabaseLoadFailureException()
        {
        }

        public DatabaseLoadFailureException(string message)
            : base(message)
        {
        }
        public DatabaseLoadFailureException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}