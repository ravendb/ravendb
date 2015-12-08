// -----------------------------------------------------------------------
//  <copyright file="ConnectionOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Connection
{
    public class ConnectionOptions
    {
        public static IDisposable Expect100Continue(Uri uri)
        {
#if !DNXCORE50
            var servicePoint = ServicePointManager.FindServicePoint(uri);
            servicePoint.Expect100Continue = true;
            return new DisposableAction(() => servicePoint.Expect100Continue = false);
#else
            // TODO [ppekrol] How to do it now?
            return new DisposableAction(() => { });
#endif
        }

        public static IDisposable Expect100Continue(string url)
        {
            return Expect100Continue(new Uri(url));
        }
    }
}
