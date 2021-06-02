// -----------------------------------------------------------------------
//  <copyright file="WidgetMessage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster
{
    public class WidgetMessage
    {
        public int Id { get; set; }
        public DynamicJsonValue Data { get; set; }
    }
}
