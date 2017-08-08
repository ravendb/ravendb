using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Primitives;

namespace Raven.Server.Config
{
    public class RavenConfigurationSection : IConfigurationSection
    {
        public RavenConfigurationSection(string key, string path, string value)
        {
            Key = key;
            Path = path;
            Value = value;
        }

        public IConfigurationSection GetSection(string key)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IConfigurationSection> GetChildren()
        {
            throw new NotImplementedException();
        }

        public IChangeToken GetReloadToken()
        {
            throw new NotImplementedException();
        }

        public string this[string key]
        {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public string Key { get; }
        public string Path { get; }
        public string Value { get; set; }
    }
}