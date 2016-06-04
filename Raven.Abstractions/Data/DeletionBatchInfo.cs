// -----------------------------------------------------------------------
//  <copyright file="DeletionBatchInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class DeletionBatchInfo
    {
        public long Id { get; set; }

        /// <summary>
        /// Index to operate on 
        /// </summary>
        public string IndexName { get; set; }

        /// <summary>
        /// Total count of documents in batch.
        /// </summary>
        public int TotalDocumentCount { get; set; }

        /// <summary>
        /// Batch processing start time.
        /// </summary>
        public DateTime StartedAt { get; set; }

        /// <summary>
        /// Total batch processing time in milliseconds.
        /// </summary>
        public double TotalDurationMs { get; set; }

        /// <summary>
        /// Performance stats
        /// </summary>
        public List<PerformanceStats> PerformanceStats { get; set; }

        public void BatchCompleted()
        {
            var now = SystemTime.UtcNow;
            TotalDurationMs = (now - StartedAt).TotalMilliseconds;
        }
    }
}