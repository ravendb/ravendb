using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Primitives;

namespace Raven.Server.Config
{
    public class RavenWebHostConfiguration : IConfiguration
    {
        private readonly RavenConfiguration _configuration;

        public RavenWebHostConfiguration(RavenConfiguration configuration)
        {
            _configuration = configuration;
        }


        public IConfigurationSection GetSection(string key)
        {
            switch (key)
            {
                case "server.urls":
                    return new RavenConfigurationSection(key, "", _configuration.Core.ServerUrl);
                default:
                    throw new NotImplementedException($"{key} should be supported");
            }
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
            get
            {
                switch (key)
                {
                    case "webroot":
                        return "webroot";
                    case "Hosting:Environment":
                        return "Production";
                    case "Hosting:Server":
                        return "Microsoft.AspNet.Server.Kestrel";
                    case "Hosting:Application":
                        return "";
                    case "server.urls":
                        return _configuration.Core.ServerUrl;
                    case "HTTP_PLATFORM_PORT":
                        return "";
                    /*var url = _configuration.Core.ServerUrls.First();
                    return url.Substring(url.IndexOf(':', "http://".Length) + 1).TrimEnd('/');*/
                    default:
                        throw new NotImplementedException($"{key} should be supported");
                }
            }
            set { throw new NotImplementedException(); }
        }
    }
}