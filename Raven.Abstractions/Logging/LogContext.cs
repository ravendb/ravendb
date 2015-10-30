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
        public static readonly ThreadLocal<string> DatabaseName = new ThreadLocal<string>();

        public static IDisposable WithDatabase(string database)
        {
            var old = DatabaseName.Value;
            var db = database ?? Constants.SystemDatabase;
            var disposable = LogManager.OpenMappedContext("database", db);
            DatabaseName.Value = db;

            return new DisposableAction(()=>
            {
                DatabaseName.Value = old;
                disposable.Dispose();
            });
        }
    }
}
