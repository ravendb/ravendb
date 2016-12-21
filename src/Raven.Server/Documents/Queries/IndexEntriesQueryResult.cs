using System;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class IndexEntriesQueryResult : QueryResultServerSide<BlittableJsonReaderObject>
    {
        public static readonly IndexEntriesQueryResult NotModifiedResult = new IndexEntriesQueryResult { NotModified = true };

        public override void AddResult(BlittableJsonReaderObject result)
        {
            Results.Add(result);
        }

        public override void HandleException(Exception e)
        {
            throw new NotSupportedException();
        }

        public override bool SupportsExceptionHandling => false;
        public override bool SupportsInclude => false;
    }
}