//-----------------------------------------------------------------------
// <copyright file="IHttpContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Security.Principal;
using log4net;

namespace Raven.Http.Abstractions
{
	public interface IHttpContext
	{
        IRaveHttpnConfiguration Configuration { get; }
		IHttpRequest Request { get; }
		IHttpResponse Response { get; }
		IPrincipal User { get; }
		void FinalizeResonse();
		void SetResponseFilter(Func<Stream, Stream> responseFilter);
		void OutputSavedLogItems(ILog logger);
		void Log(Action<ILog> loggingAction);
	}
}
