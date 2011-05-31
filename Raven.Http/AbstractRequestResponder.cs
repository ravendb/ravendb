//-----------------------------------------------------------------------
// <copyright file="AbstractRequestResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Abstractions.Data;
using Raven.Http.Abstractions;

namespace Raven.Http
{
	[InheritedExport]
	public abstract class AbstractRequestResponder
	{
		private readonly string[] supportedVerbsCached;
		protected readonly Regex urlMatcher;
        private Func<IRavenHttpConfiguration> settings;
        private Func<IResourceStore> database;

	    protected AbstractRequestResponder()
		{
			urlMatcher = new Regex(UrlPattern);
			supportedVerbsCached = SupportedVerbs;
		}

		public abstract string UrlPattern { get; }
		public abstract string[] SupportedVerbs { get; }

        public IResourceStore ResourceStore { get { return database(); } }
        public IRavenHttpConfiguration Settings { get { return settings(); } }
        public virtual bool IsUserInterfaceRequest { get { return false; } }

        public void Initialize(Func<IResourceStore> databaseGetter, Func<IRavenHttpConfiguration> settingsGetter)
        {
            this.database = databaseGetter;
            this.settings = settingsGetter;
        }

		public bool WillRespond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			return match.Success && supportedVerbsCached.Contains(context.Request.HttpMethod);
		}

        public abstract void Respond(IHttpContext context);

        protected TransactionInformation GetRequestTransaction(IHttpContext context)
        {
            var txInfo = context.Request.Headers["Raven-Transaction-Information"];
            if (string.IsNullOrEmpty(txInfo))
                return null;
            var parts = txInfo.Split(new[]{", "}, StringSplitOptions.RemoveEmptyEntries);
            if(parts.Length != 2)
                throw new ArgumentException("'Raven-Transaction-Information' is in invalid format, expected format is: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, hh:mm:ss");
            return new TransactionInformation
            {
                Id = new Guid(parts[0]),
                Timeout = TimeSpan.ParseExact(parts[1], "c", CultureInfo.InvariantCulture)
            };
        }
	}
}
