//-----------------------------------------------------------------------
// <copyright file="DatabaseDoesNotExistsException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Runtime.CompilerServices;

namespace Raven.Client.Exceptions.Database
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

        public static DatabaseDoesNotExistException CreateWithMessage(string databaseName, string message)
        {
            return new DatabaseDoesNotExistException(GetMessage(databaseName, message));
        }

        public static void Throw(string databaseName)
        {
            throw new DatabaseDoesNotExistException(GetMessage(databaseName, message: null));
        }

        public static void ThrowWithMessage(string databaseName, string message)
        {
            throw new DatabaseDoesNotExistException(GetMessage(databaseName, message));
        }

        public static void ThrowWithMessageAndException(string databaseName, string message, Exception exception)
        {
            throw new DatabaseDoesNotExistException(GetMessage(databaseName, message), exception);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string GetMessage(string databaseName, string message)
        {
            return string.IsNullOrWhiteSpace(message)
                ? $"Database '{databaseName}' does not exist."
                : $"Database '{databaseName}' does not exist. {message}";
        }
    }
}