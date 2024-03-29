﻿using System;

namespace Raven.Client.Exceptions.Database
{
    public sealed class DatabaseNotRelevantException : RavenException
    {
        public DatabaseNotRelevantException()
        {
        }

        public DatabaseNotRelevantException(string message) : base(message)
        {
        }

        public DatabaseNotRelevantException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
