// -----------------------------------------------------------------------
//  <copyright file="IProcessReplicationDocuments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Bundles.Replication.Tasks.Handlers
{
	public interface IReplicatedDocsHandler
	{
		IEnumerable<JsonDocument> Handle(IEnumerable<JsonDocument> docs);
	}
}