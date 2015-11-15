//-----------------------------------------------------------------------
// <copyright file="CurrentOperationContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Security.Principal;
using System.Threading;

namespace Raven.Database.Server
{
    public static class CurrentOperationContext
    {
        public static readonly ThreadLocal<List<IDisposable>> RequestDisposables = new ThreadLocal<List<IDisposable>>(() => new List<IDisposable>());
        public static readonly ThreadLocal<IPrincipal> User = new ThreadLocal<IPrincipal>(() => null);
        public static readonly ThreadLocal<Lazy<NameValueCollection>> Headers = new ThreadLocal<Lazy<NameValueCollection>>(() => null);

    }
}
