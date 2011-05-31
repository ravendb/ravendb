//-----------------------------------------------------------------------
// <copyright file="IRaveHttpnConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Specialized;
using System.ComponentModel.Composition.Hosting;

namespace Raven.Http
{
    public interface IRavenHttpConfiguration
    {
        string VirtualDirectory { get; }
        AnonymousUserAccessMode AnonymousUserAccessMode { get; }
        string HostName { get; }
        int Port { get; }
        CompositionContainer Container { get; set; }
        bool HttpCompression { get; }
        string WebDir { get; }
        string AccessControlAllowOrigin { get; }
        NameValueCollection Settings { get; }
    	string PluginsDirectory { get; set; }
    	string GetFullUrl(string url);
    }
}