// -----------------------------------------------------------------------
//  <copyright file="ConfigurationHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Net;

using Raven.Database.Config;
using Raven.Json.Linq;

namespace Raven.Tests.Helpers.Util
{
    public static class ConfigurationHelper
    {
        private const string ConfigurationFileName = "configuration.json";

        private static readonly object Locker = new object();

        public static bool UseFipsEncryptionAlgorithms
        {
            get
            {
                bool fips;
                bool.TryParse(ConfigurationManager.AppSettings["Raven/Encryption/FIPS"], out fips);

                return fips;
            }
        }

        private static RavenJObject _configuration;
        private static RavenJObject Configuration
        {
            get
            {
                if (_configuration != null)
                    return _configuration;

                lock (Locker)
                {
                    if (_configuration != null)
                        return _configuration;

                    return _configuration = LoadConfiguration();
                }
            }
        }

        private static Dictionary<string, NetworkCredential> _credentials;
        public static Dictionary<string, NetworkCredential> Credentials
        {
            get
            {
                if (_credentials != null)
                    return _credentials;

                lock (Locker)
                {
                    if (_credentials != null)
                        return _credentials;

                    return _credentials = ReadCredentials();
                }
            }
        }

        private static Dictionary<string, string> _settings;
        private static Dictionary<string, string> Settings
        {
            get
            {
                if (_settings != null)
                    return _settings;

                lock (Locker)
                {
                    if (_settings != null)
                        return _settings;

                    return _settings = ReadSettings();
                }
            }
        }

        public static void ApplySettingsToConfiguration(InMemoryRavenConfiguration configuration)
        {
            var settings = Settings;
            foreach (var setting in settings)
            {
                configuration.Settings[setting.Key] = setting.Value;
            }
        }

        private static Dictionary<string, string> ReadSettings()
        {
            var configuration = Configuration;
            var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (configuration.ContainsKey("settings"))
            {
                foreach (RavenJObject setting in configuration.Value<RavenJArray>("settings"))
                {
                    var key = setting.Value<string>("Key");
                    var value = setting.Value<string>("Value");

                    setting[key] = value;
                }
            }

            return settings;
        }

        private static RavenJObject LoadConfiguration()
        {
            var path = Path.Combine(@"C:\Builds", ConfigurationFileName);
            path = Path.GetFullPath(path);

            if (File.Exists(path) == false)
                return new RavenJObject();

            return RavenJObject.Parse(File.ReadAllText(path));
        }

        private static Dictionary<string, NetworkCredential> ReadCredentials()
        {
            var configuration = Configuration;
            var credentials = new Dictionary<string, NetworkCredential>(StringComparer.OrdinalIgnoreCase);
            if (configuration.ContainsKey("credentials"))
            {
                foreach (RavenJObject credential in configuration.Value<RavenJArray>("credentials"))
                {
                    var type = credential.Value<string>("Type");
                    var userName = credential.Value<string>("UserName");
                    var password = credential.Value<string>("Password");
                    var domain = credential.Value<string>("Domain");

                    credentials.Add(type, new NetworkCredential(userName, password, domain));
                }
            }

            return credentials;
        }
    }
}