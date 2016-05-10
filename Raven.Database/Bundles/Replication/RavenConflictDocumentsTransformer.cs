// -----------------------------------------------------------------------
//  <copyright file="b.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Raven.Database.Bundles.Replication
{
    public class RavenConflictDocumentsTransformer : AbstractTransformerCreationTask
    {
        public override string TransformerName
        {
            get
            {
                return "Raven/ConflictDocumentsTransformer";
            }
        }

        public override TransformerDefinition CreateTransformerDefinition(bool prettify = true)
        {
            return new TransformerDefinition
            {
                Name = TransformerName,
                TransformResults = @"
from result in results
select new {
    Id = result[""__document_id""],
    ConflictDetectedAt = result[""@metadata""].Value<DateTime>(""Last-Modified""),
                EntityName = result[""@metadata""][""Raven-Entity-Name""],
                Versions = result.Conflicts.Select(versionId =>
                {
                    var version = LoadDocument(versionId);
                    return new
                    {
                        Id = versionId,
                        SourceId = version[""@metadata""][""Raven-Replication-Source""]
                    };
                })
            }
"
            };
        }
    }
}