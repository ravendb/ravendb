// -----------------------------------------------------------------------
//  <copyright file="FileExistsException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Server.RavenFS.Storage.Exceptions
{
    public class FileExistsException : Exception
    {
        public FileExistsException(string message)
            : base(message)
        {
        }

        public FileExistsException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}