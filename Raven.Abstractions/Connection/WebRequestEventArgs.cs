//-----------------------------------------------------------------------
// <copyright file="WebRequestEventArgs.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Net;
using System.Net.Http;

namespace Raven.Abstractions.Connection
{
	/// <summary>
	/// Event arguments for the event of creating a <see cref="WebRequest"/>
	/// </summary>
	public class WebRequestEventArgs : EventArgs
	{
		/// <summary>
		/// Gets or sets the web request.
		/// </summary>
		/// <value>The request.</value>
		public HttpClient Client { get; set; }

#if !NETFX_CORE 
		/// <summary>
		/// Gets or sets the web request.
		/// </summary>
		/// <value>The request.</value>
		public WebRequest Request { get; set; }
#endif

		public OperationCredentials Credentials { get; set; }
	}
}