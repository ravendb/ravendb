using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.PeriodicExport
{
    public class PeriodicExportStatus
    {
        public DateTime LastExportAt { get; set; }
        public DateTime LastFullExportAt { get; set; }

        public long? LastDocsEtag { get; set; }

        public string LastFullExportDirectory { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastDocsEtag)] = LastExportAt,
                [nameof(LastFullExportAt)] = LastFullExportAt,
                [nameof(LastDocsEtag)] = LastDocsEtag,
                [nameof(LastFullExportDirectory)] = LastFullExportDirectory
            };
        }
    }
}