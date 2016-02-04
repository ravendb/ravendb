using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using Raven.Abstractions.Replication;
using  Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{

    public class ConnectionStringOptions
    {        
        private string url;
        public string Url
        {
            get { return url; }
            set
            {
                url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value;
            }
        }

        public string ApiKey { get; set; }

        internal string CurrentOAuthToken { get; set; }

        public ICredentials Credentials { get; set; }

        public string AuthenticationScheme { get; set; }

        public override string ToString()
        {
            var user = Credentials == null ? "<none>" : ((NetworkCredential)Credentials).UserName;
            return string.Format("Url: {0}, User: {1}, Api Key: {2}", Url, user, ApiKey);
        }
    }

    public class CounterConnectionStringOptions : ConnectionStringOptions
    {
        public string CounterStoreId { get; set; }
    }

    public class RavenConnectionStringOptions : ConnectionStringOptions
    {
        public RavenConnectionStringOptions()
        {

        }

        public string DefaultDatabase { get; set; }

        public FailoverServers FailoverServers { get; set; }

        public override string ToString()
        {
            var user = Credentials == null ? "<none>" : ((NetworkCredential)Credentials).UserName;
            return string.Format("Url: {2}, User: {0}, {1}DefaultDatabase: {2}, Api Key: {3}", user, DefaultDatabase, Url, ApiKey);
        }
    }

    public class EmbeddedRavenConnectionStringOptions : RavenConnectionStringOptions
    {
        public bool AllowEmbeddedOptions { get; set; }

        public string DataDirectory { get; set; }

        public bool RunInMemory { get; set; }
    }

    public class FilesConnectionStringOptions : ConnectionStringOptions
    {
        public string DefaultFileSystem { get; set; }

        public int MaxChunkSizeInMb { get; set; }

        public override string ToString()
        {
            var filesystem = string.IsNullOrWhiteSpace(DefaultFileSystem) ? "<none>" : DefaultFileSystem;
            return string.Format("Url: {0}, FileSystem: {1}, Api Key: {2}", Url, filesystem, ApiKey);
        }
    }

    public class ConnectionStringParser<TConnectionString> where TConnectionString : ConnectionStringOptions, new()
    {
        public static ConnectionStringParser<TConnectionString> FromConnectionString(string connectionString)
        {
            return new ConnectionStringParser<TConnectionString>("code", connectionString);
        }

        private static readonly Regex connectionStringRegex = new Regex(@"(\w+) \s* = \s* (.*)",
                                                                        RegexOptions.Compiled |
                                                                        RegexOptions.IgnorePatternWhitespace);

        private static readonly Regex connectionStringArgumentsSplitterRegex = new Regex(@"; (?=\s* \w+ \s* =)",
                                                                                         RegexOptions.Compiled |
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
        /// Parse the connection string option strictly for the ConnectionStringOptions
        /// </summary>
        protected virtual bool ProcessConnectionStringOption(ConnectionStringOptions options, NetworkCredential networkCredentials, string key, string value)
        {
            if (options == null)
                return false;

            switch (key)
            {
                case "apikey":
                    options.ApiKey = value;
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
                case "url":
                    if (string.IsNullOrEmpty(options.Url))
                        options.Url = value;
                    break;

                // Couldn't process the option.
                default: return false;
            }

            // Could process therefore we didn't enter in default.
            return true;
        }

        /// <summary>
        /// Parse the connection string option strictly for the RavenConnectionStringOptions
        /// </summary>
        protected virtual bool ProcessConnectionStringOption(RavenConnectionStringOptions options, string key, string value)
        {
            if (options == null)
                return false;

            switch (key)
            {
                case "database":
                case "defaultdatabase":
                    options.DefaultDatabase = value;
                    break;

               case "failover":
                    if (options.FailoverServers == null)
                        options.FailoverServers = new FailoverServers();

                    var databaseNameAndFailoverDestination = value.Split('|');

                    ReplicationDestination destination;
                    if (databaseNameAndFailoverDestination.Length == 1)
                    {
                        destination = JsonConvert.DeserializeObject<ReplicationDestination>(databaseNameAndFailoverDestination[0]);
                        options.FailoverServers.AddForDefaultDatabase(destination);
                    }
                    else
                    {
                        destination = JsonConvert.DeserializeObject<ReplicationDestination>(databaseNameAndFailoverDestination[1]);
                        options.FailoverServers.AddForDatabase(databaseName: databaseNameAndFailoverDestination[0], destinations: destination);
                    }
                    break;

                // Couldn't process the option.
                default: return false;
            }

            // Could process therefore we didn't enter in default.
            return true;
        }

        /// <summary>
        /// Parse the connection string option strictly for the EmbeddedRavenConnectionStringOptions
        /// </summary>
        protected virtual bool ProcessConnectionStringOption(EmbeddedRavenConnectionStringOptions options, string key, string value)
        {
            if (options == null)
                return false;

            switch (key)
            {
                case "memory":
                    bool result;
                    if (bool.TryParse(value, out result) == false)
                    {
                        throw new InvalidOperationException(string.Format("Could not understand memory setting: '{0}'", value));
                    }
                    options.RunInMemory = result;
                    break;

                case "datadir":
                    options.DataDirectory = value;
                    break;

                // Couldn't process the option.
                default: return false;
            }

            // Could process therefore we didn't enter in default.
            return true;
        }


        /// <summary>
        /// Parse the connection string option strictly for the FilesConnectionStringOptions
        /// </summary>
        protected virtual bool ProcessConnectionStringOption(FilesConnectionStringOptions options, string key, string value)
        {
            if (options == null)
                return false;

            switch (key)
            {
                case "filesystem":
                case "defaultfilesystem":
                    options.DefaultFileSystem = value;
                    break;

                // Couldn't process the option.
                default: return false;
            }

            // Could process therefore we didn't enter in default.
            return true;
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

                string key = match.Groups[1].Value.ToLower();
                string value = match.Groups[2].Value.Trim();

                // I am sure there are more elegant solutions than this one. But it makes the job done. 
                // Clear separation and same parsing logic as long as inheritance tree is well constructed and the calls are topologically ordered.
                bool processed = ProcessConnectionStringOption(ConnectionStringOptions, networkCredential, key, value);
                processed |= ProcessConnectionStringOption(ConnectionStringOptions as RavenConnectionStringOptions, key, value);
                processed |= ProcessConnectionStringOption(ConnectionStringOptions as EmbeddedRavenConnectionStringOptions, key, value);
                processed |= ProcessConnectionStringOption(ConnectionStringOptions as FilesConnectionStringOptions, key, value);

                if (!processed)
                    throw new ArgumentException(string.Format("Connection string name: '{0}' could not be parsed, unknown option: '{1}'", connectionStringName, key));
            }

            if (setupUsernameInConnectionString == false && setupPasswordInConnectionString == false)
                return;

            if (setupUsernameInConnectionString == false || setupPasswordInConnectionString == false)
                throw new ArgumentException(string.Format("User and Password must both be specified in the connection string: '{0}'", connectionStringName));
            
            ConnectionStringOptions.Credentials = networkCredential;
        }
    }
}
