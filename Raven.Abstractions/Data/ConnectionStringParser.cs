#if !NETFX_CORE
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;

namespace Raven.Abstractions.Data
{
	public class RavenConnectionStringOptions
	{
		public RavenConnectionStringOptions()
		{
#if MONO
			EnlistInDistributedTransactions = false;
#else
			EnlistInDistributedTransactions = true;
#endif
		}

		public ICredentials Credentials { get; set; }
		public bool EnlistInDistributedTransactions { get; set; }
		public string DefaultDatabase { get; set; }
		public Guid ResourceManagerId { get; set; }
		private string url;

		public string Url
		{
			get { return url; }
			set
			{
				url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value;
			}
		}

		public FailoverServers FailoverServers { get; set; }

		public string ApiKey { get; set; }

		internal string CurrentOAuthToken { get; set; }

		public override string ToString()
		{
			var user = Credentials == null ? "<none>" : ((NetworkCredential)Credentials).UserName;
			return string.Format("Url: {4}, User: {0}, EnlistInDistributedTransactions: {1}, DefaultDatabase: {2}, ResourceManagerId: {3}, Api Key: {5}", user, EnlistInDistributedTransactions, DefaultDatabase, ResourceManagerId, Url, ApiKey);
		}
	}

	public class EmbeddedRavenConnectionStringOptions : RavenConnectionStringOptions
	{
		public bool AllowEmbeddedOptions { get; set; }

		public string DataDirectory { get; set; }

		public bool RunInMemory { get; set; }
	}

	public class ConnectionStringParser<TConnectionString> where TConnectionString : RavenConnectionStringOptions, new()
	{
		public static ConnectionStringParser<TConnectionString> FromConnectionStringName(string connectionStringName)
		{
#if !MONODROID && !SILVERLIGHT
			var connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];
			if (connectionStringSettings == null)
				throw new ArgumentException(string.Format("Could not find connection string name: '{0}'", connectionStringName));

		
			return new ConnectionStringParser<TConnectionString>(connectionStringName, connectionStringSettings.ConnectionString);
#else
			throw new ArgumentException(string.Format("Connection string not supported"));
#endif
		}

		public static ConnectionStringParser<TConnectionString> FromConnectionString(string connectionString)
		{
			return new ConnectionStringParser<TConnectionString>("code", connectionString);
		}

		private static readonly Regex connectionStringRegex = new Regex(@"(\w+) \s* = \s* (.*)",
#if !SILVERLIGHT
		                                                                RegexOptions.Compiled |
#endif
		                                                                RegexOptions.IgnorePatternWhitespace);

		private static readonly Regex connectionStringArgumentsSplitterRegex = new Regex(@"; (?=\s* \w+ \s* =)",
#if !SILVERLIGHT
		                                                                                 RegexOptions.Compiled |
#endif
		                                                                                 RegexOptions.IgnorePatternWhitespace);

		private readonly string connectionString;
		private readonly string connectionStringName;

		private bool setupPasswordInConnectionString;
		private bool setupUsernameInConnectionString;

		public TConnectionString ConnectionStringOptions { get; set; }

		private ConnectionStringParser(string connectionStringName, string connectionString)
		{
			ConnectionStringOptions = new TConnectionString();
			this.connectionString = connectionString;
			this.connectionStringName = connectionStringName;
		}

		/// <summary>
		/// Parse the connection string option
		/// </summary>
		protected virtual void ProcessConnectionStringOption(NetworkCredential networkCredentials, string key, string value)
		{
			var embeddedRavenConnectionStringOptions = ConnectionStringOptions as EmbeddedRavenConnectionStringOptions;
			switch (key)
			{
				case "apikey":
					ConnectionStringOptions.ApiKey = value;
					break;
				case "memory":
					if(embeddedRavenConnectionStringOptions  == null)
						goto default;
					bool result;
					if (bool.TryParse(value, out result) == false)
					{
#if !MONODROID && !SILVERLIGHT
						throw new ConfigurationErrorsException(string.Format("Could not understand memory setting: '{0}'", value));
#else
						throw new Exception(string.Format("Could not understand memory setting: '{0}'", value));
#endif
					}
					embeddedRavenConnectionStringOptions.RunInMemory = result;
					break;
				case "datadir":
					if(embeddedRavenConnectionStringOptions  == null)
						goto default;

					embeddedRavenConnectionStringOptions.DataDirectory = value;
					break;
				case "enlist":
					ConnectionStringOptions.EnlistInDistributedTransactions = bool.Parse(value);
					break;
				case "resourcemanagerid":
					ConnectionStringOptions.ResourceManagerId = new Guid(value);
					break;
				case "url":
					if (string.IsNullOrEmpty(ConnectionStringOptions.Url))
						ConnectionStringOptions.Url = value;
					break;
				case "failoverurl":
					if (ConnectionStringOptions.FailoverServers == null)
						ConnectionStringOptions.FailoverServers = new FailoverServers();

					var databaseNameAndFailoverUrl = value.Split('|');

					if (databaseNameAndFailoverUrl.Length == 1)
					{
						ConnectionStringOptions.FailoverServers.AddForDefaultDatabase(urls: databaseNameAndFailoverUrl[0]);
					}
					else
					{
						ConnectionStringOptions.FailoverServers.AddForDatabase(databaseName: databaseNameAndFailoverUrl[0], urls: databaseNameAndFailoverUrl[1]);
					}
					break;
				case "database":
				case "defaultdatabase":
					ConnectionStringOptions.DefaultDatabase = value;
					break;
				case "user":
					networkCredentials.UserName = value;
					setupUsernameInConnectionString = true;
					break;
				case "password":
					networkCredentials.Password = value;
					setupPasswordInConnectionString = true;
					break;
				case "domain":
					networkCredentials.Domain = value;
					break;
				default:
					throw new ArgumentException(string.Format("Connection string name: '{0}' could not be parsed, unknown option: '{1}'", connectionStringName, key));
			}
		}

		public void Parse()
		{
			string[] strings = connectionStringArgumentsSplitterRegex.Split(connectionString);
			var networkCredential = new NetworkCredential();
			foreach (string str in strings)
			{
				string arg = str.Trim(';');
				Match match = connectionStringRegex.Match(arg);
				if (match.Success == false)
					throw new ArgumentException(string.Format("Connection string name: '{0}' could not be parsed", connectionStringName));
				ProcessConnectionStringOption(networkCredential, match.Groups[1].Value.ToLower(), match.Groups[2].Value.Trim());
			}

			if (setupUsernameInConnectionString == false && setupPasswordInConnectionString == false)
				return;

			if (setupUsernameInConnectionString == false || setupPasswordInConnectionString == false)
				throw new ArgumentException(string.Format("User and Password must both be specified in the connection string: '{0}'", connectionStringName));
			ConnectionStringOptions.Credentials = networkCredential;
		}
	}
}
#endif
