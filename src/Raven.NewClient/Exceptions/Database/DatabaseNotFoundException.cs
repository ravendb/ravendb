//-----------------------------------------------------------------------
// <copyright file="DatabaseDoesNotExistsException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.NewClient.Client.Exceptions.Database
{
    public class DatabaseNotFoundException : RavenException
    {
        public DatabaseNotFoundException()
        {
        }

        public DatabaseNotFoundException(string message)
            : base(message)
        {
        }
    }
}