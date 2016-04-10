// -----------------------------------------------------------------------
//  <copyright file="CultureHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;

using Raven.Abstractions.Extensions;

namespace Raven.Server.Utils
{
    public static class CultureHelper
    {
        internal static IDisposable EnsureInvariantCulture()
        {
            if (CultureInfo.CurrentCulture.Equals(CultureInfo.InvariantCulture))
                return null;

            var oldCurrentCulture = CultureInfo.CurrentCulture;
            var oldCurrentUiCulture = CultureInfo.CurrentUICulture;

            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;
            return new DisposableAction(() =>
            {
                CultureInfo.CurrentCulture = oldCurrentCulture;
                CultureInfo.CurrentUICulture = oldCurrentUiCulture;
            });
        }
    }
}