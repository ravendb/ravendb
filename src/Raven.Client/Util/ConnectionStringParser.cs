using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Client.Util
{
    public class ConnectionStringOptions
    {
        public List<string> Urls { get; set; }

        public string ApiKey { get; set; }

        public override string ToString()
        {
            return string.Format("Urls: {0}, Api Key: {1}", string.Join(",", Urls), ApiKey);
        }
    }

    public class RavenConnectionStringOptions : ConnectionStringOptions
    {
        public string Database { get; set; }

        public override string ToString()
        {
            return string.Format("Urls: {1}, Database: {0}, Api Key: {2}", Database, string.Join(",", Urls), ApiKey);
        }
    }

    public class ConnectionStringParser<TConnectionString> where TConnectionString : ConnectionStringOptions, new()
    {
        public static ConnectionStringParser<TConnectionString> FromConnectionString(string connectionString)
        {
            return new ConnectionStringParser<TConnectionString>("code", connectionString);
        }

        private static readonly Regex ConnectionStringRegex = new Regex(@"(\w+) \s* = \s* (.*)",
                                                                        RegexOptions.Compiled |
                                                                        RegexOptions.IgnorePatternWhitespace);

        private static readonly Regex UrlStringRegex = new Regex(@"^http(s?)\:\/\/[0-9a-zA-Z]([-.\w]*[0-9a-zA-Z])*(:(0-9)*)*(\/?)([a-zA-Z0-9\-\.\?\,\'\/\\\+&amp;%\$#_]*)?$",
            RegexOptions.Compiled |
            RegexOptions.IgnorePatternWhitespace);

        private static readonly Regex ConnectionStringArgumentsSplitterRegex = new Regex(@"; (?=\s* \w+ \s* =)",
                                                                                         RegexOptions.Compiled |
                                                                                         RegexOptions.IgnorePatternWhitespace);

        private static readonly Regex ConnectionStringListSplitterRegex = new Regex(@"(\s?,\s?)",
            RegexOptions.Compiled |
            RegexOptions.IgnorePatternWhitespace);

        private readonly string _connectionString;
        private readonly string _connectionStringName;

        public TConnectionString ConnectionStringOptions { get; set; }

        private ConnectionStringParser(string connectionStringName, string connectionString)
        {
            ConnectionStringOptions = new TConnectionString();
            _connectionString = connectionString;
            _connectionStringName = connectionStringName;
        }

        /// <summary>
        /// Parse the connection string option strictly for the ConnectionStringOptions
        /// </summary>
        protected virtual bool ProcessConnectionStringOption(ConnectionStringOptions options, string key, string value)
        {
            if (options == null)
                return false;

            switch (key)
            {
                case "apikey":
                    options.ApiKey = value;
                    break;
                case "urls":
                    if (options.Urls == null || options.Urls?.Count == 0)
                    {
                        var items = ConnectionStringListSplitterRegex.Split(value);
                        if (options.Urls == null)
                            options.Urls = new List<string>();

                        foreach (var item in items)
                        {
                            Match match = UrlStringRegex.Match(item);
                            if (match.Success == false)
                                throw new ArgumentException(string.Format("url: '{0}' could not be parsed", item));

                            options.Urls.Add(item);
                        }

                    }
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
                    options.Database = value;
                    break;

                // Couldn't process the option.
                default: return false;
            }

            // Could process therefore we didn't enter in default.
            return true;
        }

        public void Parse()
        {
            var strings = ConnectionStringArgumentsSplitterRegex.Split(_connectionString);
            foreach (var str in strings)
            {
                var arg = str.Trim(';');
                var match = ConnectionStringRegex.Match(arg);
                if (match.Success == false)
                    throw new ArgumentException(string.Format("Connection string name: '{0}' could not be parsed", _connectionStringName));

                var key = match.Groups[1].Value.ToLower();
                var value = match.Groups[2].Value.Trim();

                // I am sure there are more elegant solutions than this one. But it makes the job done. 
                // Clear separation and same parsing logic as long as inheritance tree is well constructed and the calls are topologically ordered.
                var processed = ProcessConnectionStringOption(ConnectionStringOptions, key, value);
                processed |= ProcessConnectionStringOption(ConnectionStringOptions as RavenConnectionStringOptions, key, value);

                if (processed == false)
                    throw new ArgumentException(string.Format("Connection string name: '{0}' could not be parsed, unknown option: '{1}'", _connectionStringName, key));
            }
        }
    }
}
