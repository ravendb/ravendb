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
using Raven.Abstractions.Threading;

namespace Raven.Database.Server
{
	public static class CurrentOperationContext
	{
		public static readonly Raven.Abstractions.Threading.ThreadLocal<List<IDisposable>> RequestDisposables = new Raven.Abstractions.Threading.ThreadLocal<List<IDisposable>>(() => new List<IDisposable>());
		public static readonly Raven.Abstractions.Threading.ThreadLocal<IPrincipal> User = new Raven.Abstractions.Threading.ThreadLocal<IPrincipal>(() => null);
        public static readonly Raven.Abstractions.Threading.ThreadLocal<Lazy<NameValueCollection>> Headers = new Raven.Abstractions.Threading.ThreadLocal<Lazy<NameValueCollection>>(() => null);

	}
}
