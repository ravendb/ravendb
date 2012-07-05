//-----------------------------------------------------------------------
// <copyright file="CurrentOperationContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Specialized;
using System.Security.Principal;
using System.Threading;

namespace Raven.Database.Server
{
	public static class CurrentOperationContext
	{
		public static readonly ThreadLocal<IPrincipal> User = new ThreadLocal<IPrincipal>(() => null);
		public static readonly ThreadLocal<NameValueCollection> Headers = new ThreadLocal<NameValueCollection>(() => new NameValueCollection());
	}
}
