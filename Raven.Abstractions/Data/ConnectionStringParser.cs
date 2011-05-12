using System;
using System.Configuration;
using System.Net;
using System.Text.RegularExpressions;

namespace Raven.Abstractions.Data
{
	public class ConnectionStringParser
	{
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

		public ConnectionStringParser(string connectionString, string connectionStringName)
		{
			this.connectionString = connectionString;
			this.connectionStringName = connectionStringName;
		}

		public NetworkCredential Credentials { get; set; }
		
		public bool EnlistInDistributedTransactions { get; set; }

		public string DefaultDatabase { get; set; }

		public Guid ResourceManagerId { get; set; }

		public string Url { get; set; }

		/// <summary>
		/// Parse the connection string option
		/// </summary>
		protected virtual void ProcessConnectionStringOption(NetworkCredential neworkCredentials, string key, string value)
		{
			switch (key)
			{
				case "memory":
					if(AllowEmbeddedOptions == false)
						goto default;
					bool result;
					if (bool.TryParse(value, out result) == false)
						throw new ConfigurationErrorsException("Could not understand memory setting: " +
															   value);
					RunInMemory = result;
					break;
				case "datadir":
					if(AllowEmbeddedOptions == false)
						goto default;
					
						DataDirectory = value;
					break;
				case "enlist":
					EnlistInDistributedTransactions = bool.Parse(value);
					break;
				case "resourcemanagerid":
					ResourceManagerId = new Guid(value);
					break;
				case "url":
					Url = value;
					break;
				case "database":
				case "defaultdatabase":
					DefaultDatabase = value;
					break;
				case "user":
					neworkCredentials.UserName = value;
					setupUsernameInConnectionString = true;
					break;
				case "password":
					neworkCredentials.Password = value;
					setupPasswordInConnectionString = true;
					break;

				default:
					throw new ArgumentException("Connection string name: " + connectionStringName +
					                            " could not be parsed, unknown option: " + key);
			}
		}

		public bool AllowEmbeddedOptions { get; set; }

		public string DataDirectory { get; set; }

		public bool RunInMemory { get; set; }

		public void Parse()
		{
			string[] strings = connectionStringArgumentsSplitterRegex.Split(connectionString);
			var networkCredential = new NetworkCredential();
			foreach (string str in strings)
			{
				string arg = str.Trim(';');
				Match match = connectionStringRegex.Match(arg);
				if (match.Success == false)
					throw new ArgumentException("Connection string name: " + connectionStringName + " could not be parsed");
				ProcessConnectionStringOption(networkCredential, match.Groups[1].Value.ToLower(), match.Groups[2].Value.Trim());
			}

			if (setupUsernameInConnectionString == false && setupPasswordInConnectionString == false)
				return;

			if (setupUsernameInConnectionString == false || setupPasswordInConnectionString == false)
				throw new ArgumentException("User and Password must both be specified in the connection string: " +
				                            connectionStringName);
			Credentials = networkCredential;
		}
	}
}