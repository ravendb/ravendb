using System;
using System.Globalization;
using System.Linq;
using Raven.Client.ServerWide;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.BackgroundWork;

public abstract class AbstractBackgroundWorkStorageBase
{
    public static bool ShouldHandleWorkOnCurrentNode(DatabaseTopology topology, string nodeTag)
    {
        var isFirstInTopology = string.Equals(topology.AllNodes.FirstOrDefault(), nodeTag, StringComparison.OrdinalIgnoreCase);
        if (isFirstInTopology == false)
        {
            // this can happen when we are running the expiration/refresh/data archival on a node that isn't 
            // the primary node for the database. In this case, we still run the cleanup
            // procedure, but we only account for documents that have already been 
            // marked for processing, to cleanup the queue. We'll stop on the first
            // document that is scheduled to be processed (expired/refreshed/archived) and wait until the 
            // primary node will act on it. In this way, we reduce conflicts between nodes
            // performing the same action concurrently.     
            return false;
        }

        return true;
    }

    public static unsafe bool HasPassed(BlittableJsonReaderObject metadata, DateTime currentTime, string metadataPropertyToCheck)
    {
        if (metadata.TryGet(metadataPropertyToCheck, out LazyStringValue dateFromMetadata) == false)
            return false;

        if (LazyStringParser.TryParseDateTime(dateFromMetadata.Buffer, dateFromMetadata.Length, out DateTime date, out _, properlyParseThreeDigitsMilliseconds: true) != LazyStringParser.Result.DateTime)
            if (DateTime.TryParseExact(dateFromMetadata.ToString(CultureInfo.InvariantCulture), DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind, out date) == false)
                return false;

        if (date.Kind != DateTimeKind.Utc)
            date = date.ToUniversalTime();

        return currentTime >= date;
    }
}
