using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Utils;

namespace Raven.Server.Documents;

public class AdditionalPatchInformation
{
    private readonly QueryOperationOptions _options;
    private readonly BulkOperationResult _result;
    private readonly string _dbBase64Id;

    public HashSet<string> Collections { get; } = new(StringComparer.OrdinalIgnoreCase);

    public long LastEtag { get; set; }

    public AdditionalPatchInformation(QueryOperationOptions options, BulkOperationResult result, string dbBase64Id)
    {
        _options = options;
        _result = result;
        _dbBase64Id = dbBase64Id;
    }

    public void RetrieveDetails(IBulkOperationDetails details)
    {
        if (_options.IndexPatchOptions != null)
        {
            switch (details)
            {
                case BulkOperationResult.DeleteDetails d:
                    Collections.Add(d.Collection);
                    if (d.Etag.HasValue)
                        LastEtag = d.Etag.Value;
                    break;
                case BulkOperationResult.PatchDetails p:
                    Collections.Add(p.Collection);
                    LastEtag = p.Etag;
                    break;
            }
        }
        if (_options.RetrieveDetails && _result?.Details != null)
            _result.Details.Add(details);
    }
}
