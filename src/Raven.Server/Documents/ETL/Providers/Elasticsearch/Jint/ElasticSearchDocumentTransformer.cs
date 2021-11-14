using System;
using System.Collections.Generic;
using Jint.Native;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    internal partial class ElasticSearchDocumentTransformer
    {
        protected override void AddLoadedAttachmentJint(JsValue reference, string name, Attachment attachment)
        {
            throw new NotSupportedException("Attachments aren't supported by ElasticSearch ETL");
        }

        protected override void AddLoadedCounterJint(JsValue reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by ElasticSearch ETL");
        }

        protected override void AddLoadedTimeSeriesJint(JsValue reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by ElasticSearch ETL");
        }

    }
}
