// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesDetail.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesStats
    {
        public string DocumentId { get; set; }
        
        public List<TimeSeriesItemDetail> TimeSeries { get; set; }

        public TimeSeriesStats()
        {
            TimeSeries = new List<TimeSeriesItemDetail>();
        }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(TimeSeries)] = new DynamicJsonArray(TimeSeries.Select(x => x.ToJson()))
            };
        }
    }

    public class TimeSeriesItemDetail
    {
        public string Name { get; set; }
        public long NumberOfEntries { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(NumberOfEntries)] = NumberOfEntries,
                [nameof(StartDate)] = StartDate,
                [nameof(EndDate)] = EndDate
            };
        }
        
    }
}
