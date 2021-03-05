// -----------------------------------------------------------------------
//  <copyright file="Widget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading;
using Raven.Server.Background;

namespace Raven.Server.NotificationCenter.Widgets
{
    /// <summary>
    /// Base class for Cluster Dashboard Widgets w/o configuration
    /// </summary>
    public abstract class Widget : BackgroundWorkBase
    {
        protected Widget(int id, CancellationToken shutdown) : base("ClusterDashboardWidget", shutdown)
        {
            Id = id;
        }
        
        public int Id { get; }

        public abstract WidgetType Type { get; }
    }
    
    /// <summary>
    /// Base class for Cluster Dashboard Widgets with Configuration 
    /// </summary>
    /// <typeparam name="TConfig">Configuration Type</typeparam>
    public abstract class Widget<TConfig> : Widget
    {
        public TConfig Configuration { get; }

        protected Widget(int id, TConfig configuration, CancellationToken shutdown) : base(id, shutdown)
        {
            Configuration = configuration;
        }
    }
}
