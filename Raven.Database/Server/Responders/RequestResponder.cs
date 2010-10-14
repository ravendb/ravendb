using System;
using System.ComponentModel.Composition;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	[InheritedExport]
	public abstract class RequestResponder
	{
		private readonly string[] supportedVerbsCached;
		protected readonly Regex urlMatcher;
        private Func<InMemroyRavenConfiguration> settings;
	    private Func<DocumentDatabase> database;

	    protected RequestResponder()
		{
			urlMatcher = new Regex(UrlPattern);
			supportedVerbsCached = SupportedVerbs;
		}

		public abstract string UrlPattern { get; }
		public abstract string[] SupportedVerbs { get; }

        public DocumentDatabase Database { get { return database(); } }
        public InMemroyRavenConfiguration Settings { get { return settings(); } }

        public void Initialize(Func<DocumentDatabase> databaseGetter, Func<InMemroyRavenConfiguration> settingsGetter)
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
