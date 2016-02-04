// -----------------------------------------------------------------------
//  <copyright file="l.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Server.Config
{
    public class LowMemoryHandlerStatistics
    {
        public string Name { get; set; }
        public long EstimatedUsedMemory { get; set; }
        public string DatabaseName { get; set; }
        public object Metadata { get; set; }
    }
}