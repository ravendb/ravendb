// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Raven.Database.Bundles.Replication
{
    public class RavenConflictDocuments : AbstractIndexCreationTask
    {
        public override string IndexName
        {
            get
            {
                return "Raven/ConflictDocuments";
            }
        }
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Name = IndexName,
                Map = @"
from doc in docs
let id = doc[""@metadata""][""@id""]
where doc[""@metadata""][""Raven-Replication-Conflict""] == true && (id.Length < 47 || !id.Substring(id.Length - 47).StartsWith(""/conflicts/"", StringComparison.OrdinalIgnoreCase))
select new
{
    ConflictDetectedAt = (DateTime)doc[""@metadata""][""Last-Modified""]
}
"
            };
        }
    }
}