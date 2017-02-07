#if !DNXCORE50
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
        private static readonly ThreadLocal<string> resourceName = new ThreadLocal<string>();

        public static IDisposable WithResource(string resourceName)
        {
            var old = LogContext.resourceName.Value;
            var name = resourceName ?? Constants.SystemDatabase;
            var disposable = LogManager.OpenMappedContext("resource", name);
            LogContext.resourceName.Value = name;

            return new DisposableAction(()=>
            {
                LogContext.resourceName.Value = old;
                disposable.Dispose();
            });
        }

        public static string ResourceName
        {
            get
            {
                try
                {
                    return resourceName.Value;
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
                    resourceName.Value = value;
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }
    }
}
#endif