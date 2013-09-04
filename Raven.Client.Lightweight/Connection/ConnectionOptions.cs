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
		public static IDisposable Expect100Continue(string url)
		{
#if SILVERLIGHT || NETFX_CORE
			return new DisposableAction(() => { });
#else
			var servicePoint = ServicePointManager.FindServicePoint(new Uri(url));
			servicePoint.Expect100Continue = true;
			return new DisposableAction(() => servicePoint.Expect100Continue = false);
#endif
		} 
	}
}