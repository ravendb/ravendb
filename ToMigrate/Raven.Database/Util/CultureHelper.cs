// -----------------------------------------------------------------------
//  <copyright file="CultureHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Threading;

using Raven.Abstractions.Extensions;

namespace Raven.Database.Util
{
    public static class CultureHelper
    {
        internal static IDisposable EnsureInvariantCulture()
        {
            if (Thread.CurrentThread.CurrentCulture == CultureInfo.InvariantCulture)
                return null;

            var oldCurrentCulture = Thread.CurrentThread.CurrentCulture;
            var oldCurrentUiCulture = Thread.CurrentThread.CurrentUICulture;

            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            return new DisposableAction(() =>
            {
                Thread.CurrentThread.CurrentCulture = oldCurrentCulture;
                Thread.CurrentThread.CurrentUICulture = oldCurrentUiCulture;
            });
        } 
    }
}
