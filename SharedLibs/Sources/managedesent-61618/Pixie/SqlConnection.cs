//-----------------------------------------------------------------------
// <copyright file="SqlConnection.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Parse a SQL statement and execute the commands.
    /// </summary>
    public interface SqlConnection : IDisposable
    {
        /// <summary>
        /// Parse and execute the SQL command.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        void Execute(string command);
    }
}