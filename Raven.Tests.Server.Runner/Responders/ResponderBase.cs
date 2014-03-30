// -----------------------------------------------------------------------
//  <copyright file="ResponderBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Server.Runner.Responders
{
	using System;
	using System.Linq;
	using System.Text.RegularExpressions;

	using Raven.Database.Server.Abstractions;

	public abstract class ResponderBase : IDisposable
	{
		private readonly Regex urlMatcher;

		protected ResponderBase()
		{
			urlMatcher = new Regex(UrlPattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);
		}

		public abstract string UrlPattern { get; }

		public abstract string[] SupportedVerbs { get; }

		public abstract void Respond(IHttpContext context);

		public bool WillRespond(IHttpContext context)
		{
			var requestUrl = context.GetRequestUrl();
			var match = urlMatcher.Match(requestUrl);
			return match.Success && SupportedVerbs.Contains(context.Request.HttpMethod);
		}

		public abstract void Dispose();
	}
}