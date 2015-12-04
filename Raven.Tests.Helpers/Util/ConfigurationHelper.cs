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
using System.Reflection;

using Raven.Client;
using Raven.Client.Document;
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

        private static Dictionary<string, string> _serverSettings;
        private static Dictionary<string, string> ServerSettings
        {
            get
            {
                if (_serverSettings != null)
                    return _serverSettings;

                lock (Locker)
                {
                    if (_serverSettings != null)
                        return _serverSettings;

                    return _serverSettings = ReadSettings("server");
                }
            }
        }

        private static Dictionary<string, string> _clientSettings;
        private static Dictionary<string, string> ClientSettings
        {
            get
            {
                if (_clientSettings != null)
                    return _clientSettings;

                lock (Locker)
                {
                    if (_clientSettings != null)
                        return _clientSettings;

                    return _clientSettings = ReadSettings("client");
                }
            }
        }

        public static void ApplySettingsToConfiguration(InMemoryRavenConfiguration configuration)
        {
            var settings = ServerSettings;
            foreach (var setting in settings)
            {
                Console.WriteLine("Applying external server setting: " + setting.Key);

                configuration.Settings[setting.Key] = setting.Value;
            }

            configuration.Initialize();
        }

        public static void ApplySettingsToConventions(Convention conventions)
        {
            var settings = ClientSettings;

            if (settings.Count == 0)
                return;

            var type = conventions.GetType();

            foreach (var setting in settings)
            {
                Console.WriteLine("Applying external client setting: " + setting.Key);

                var property = type.GetProperty(setting.Key, BindingFlags.Public | BindingFlags.Instance);
                if (property == null)
                    continue;

                property.SetValue(conventions, setting.Value);
            }
        }

        private static Dictionary<string, string> ReadSettings(string sectionName)
        {
            var configuration = Configuration;
            var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (configuration.ContainsKey(sectionName))
            {
                var settingsJson = configuration.Value<RavenJObject>(sectionName);
                foreach (var settingKey in settingsJson.Keys)
                {
                    settings[settingKey] = settingsJson.Value<string>(settingKey);
                }
            }

            return settings;
        }

        private static Dictionary<string, NetworkCredential> ReadCredentials()
        {
            var configuration = Configuration;
            var credentials = new Dictionary<string, NetworkCredential>(StringComparer.OrdinalIgnoreCase);
            if (configuration.ContainsKey("credentials"))
            {
                var credentialsJson = configuration.Value<RavenJObject>("credentials");

                foreach (var credentialKey in credentialsJson.Keys)
                {
                    var credential = credentialsJson.Value<RavenJObject>(credentialKey);

                    var userName = credential.Value<string>("UserName");
                    var password = credential.Value<string>("Password");
                    var domain = credential.Value<string>("Domain");

                    credentials.Add(credentialKey, new NetworkCredential(userName, password, domain));
                }
            }

            return credentials;
        }

        private static RavenJObject LoadConfiguration()
        {
            var path = Path.Combine(@"C:\Builds", ConfigurationFileName);
            path = Path.GetFullPath(path);

            if (File.Exists(path) == false)
                return new RavenJObject();

            return RavenJObject.Parse(File.ReadAllText(path));
        }
    }
}