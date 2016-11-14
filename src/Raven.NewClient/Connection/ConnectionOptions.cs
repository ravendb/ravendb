// -----------------------------------------------------------------------
//  <copyright file="ConnectionOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using Raven.NewClient.Abstractions;
using Raven.NewClient.Abstractions.Extensions;

namespace Raven.NewClient.Client.Connection
{
    public static class ConnectionOptions
    {
        private static IDisposable Expect100Continue(Uri uri)
        {
            if(SystemTime.UtcNow > new DateTime(2016,2,6))
                throw new TimeoutException("You should have fixed me by now: RavenDB-4186");
            return new DisposableAction(() => { });
        }

        public static IDisposable Expect100Continue(string url)
        {
            return Expect100Continue(new Uri(url));
        }
    }
}
