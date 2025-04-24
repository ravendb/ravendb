using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Utils;

namespace Raven.Server.Documents;

public class WaitForIndexesInformation
{
    private readonly QueryOperationOptions _options;
    private readonly BulkOperationResult _result;
    private readonly DocumentDatabase _database;

    public HashSet<string> Collections { get; } = new();

    public long LastEtag { get; set; }

    public WaitForIndexesInformation(QueryOperationOptions options, BulkOperationResult result, DocumentDatabase database)
    {
        _options = options;
        _result = result;
        _database = database;
    }

    public void RetrieveDetails(IBulkOperationDetails details)
    {
        if (_options.WaitForIndexingAfterPatchOptions != null)
        {
            switch (details)
            {
                case BulkOperationResult.DeleteDetails d:
                    Collections.Add(d.Collection);
                    if (d.Etag.HasValue)
                        LastEtag = Math.Max(d.Etag.Value, LastEtag);
                    break;
                case BulkOperationResult.PatchDetails p:
                    Collections.Add(p.Collection);
                    LastEtag = ChangeVectorUtils.GetEtagById(p.ChangeVector, _database.DbBase64Id);
                    break;
            }
        }
        if (_options.RetrieveDetails && _result != null)
            _result.Details.Add(details);
    }
}
