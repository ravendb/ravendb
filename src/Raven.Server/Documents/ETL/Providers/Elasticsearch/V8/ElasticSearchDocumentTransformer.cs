using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    internal partial class ElasticSearchDocumentTransformer
    {
        protected override void AddLoadedAttachmentV8(InternalHandle reference, string name, Attachment attachment)
        {
            throw new NotSupportedException("Attachments aren't supported by ElasticSearch ETL");
        }

        protected override void AddLoadedCounterV8(InternalHandle reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by ElasticSearch ETL");
        }

        protected override void AddLoadedTimeSeriesV8(InternalHandle reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by ElasticSearch ETL");
        }
    }
}
