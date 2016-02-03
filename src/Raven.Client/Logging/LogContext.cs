//-----------------------------------------------------------------------
// <copyright file="CurrentOperationContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Logging
{
    public static class LogContext
    {
        private static readonly ThreadLocal<string> databaseName = new ThreadLocal<string>();

        public static IDisposable WithDatabase(string database)
        {
            var old = databaseName.Value;
            var disposable = LogManager.OpenMappedContext("database", database);
            databaseName.Value = database;

            return new DisposableAction(()=>
            {
                databaseName.Value = old;
                disposable.Dispose();
            });
        }

        public static string DatabaseName
        {
            get
            {
                try
                {
                    return databaseName.Value;
                }
                catch (ObjectDisposedException)
                {
                    // can happen when logging from finalizers under crash scenario
                    return "unknown";
                }
            }
            set
            {
                try
                {
                    databaseName.Value = value;
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }
    }
}
