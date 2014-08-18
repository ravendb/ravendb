// -----------------------------------------------------------------------
//  <copyright file="IndexStalenessDetectionOptimizer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
	public class HighestEtagsPerCollection : AbstractPutTrigger
	{
		public override void AfterCommit(string key, RavenJObject document, RavenJObject metadata, Etag etag)
		{
			var entityName = metadata.Value<string>(Constants.RavenEntityName);

			if(entityName == null)
				return;

			Database.IndexingExecuter.HighestDocumentEtagPerCollection.AddOrUpdate(entityName, etag, (existingEntity, existingEtag) => etag.CompareTo(existingEtag) > 0 ? etag : existingEtag);
		}
	}
}