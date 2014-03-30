// -----------------------------------------------------------------------
//  <copyright file="FileSystemMetrics.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;

namespace Raven.Abstractions.RavenFS
{
    public class FileSystemMetrics
    {
        public double FilesWritesPerSecond { get; set; }

        public double RequestsPerSecond { get; set; }

        public MeterData Requests { get; set; }

        public HistogramData RequestsDuration { get; set; }
    }
}