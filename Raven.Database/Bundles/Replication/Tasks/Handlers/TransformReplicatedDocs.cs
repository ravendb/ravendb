// -----------------------------------------------------------------------
//  <copyright file="PatchReplicatedDocs.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Json;

namespace Raven.Database.Bundles.Replication.Tasks.Handlers
{
	public class TransformReplicatedDocs : IReplicatedDocsHandler
	{
		private readonly static ILog Log = LogManager.GetCurrentClassLogger();

		private readonly DocumentDatabase database;
		private readonly ReplicationStrategy strategy;

		public TransformReplicatedDocs(DocumentDatabase database, ReplicationStrategy strategy)
		{
			this.database = database;
			this.strategy = strategy;
		}

		public IEnumerable<JsonDocument> Handle(IEnumerable<JsonDocument> docs)
		{
			if (strategy.TransformScripts == null || strategy.TransformScripts.Count == 0)
				return docs;

			return docs.Select(doc =>
			{
				var collection = doc.Metadata.Value<string>(Constants.RavenEntityName);

				string script;
				if (strategy.TransformScripts.TryGetValue(collection, out script) == false)
					return doc;

				var patcher = new ScriptedJsonPatcher(database);
				using (var scope = new DefaultScriptedJsonPatcherOperationScope(database))
				{
					try
					{
						doc.DataAsJson = patcher.Apply(scope, doc.DataAsJson, new ScriptedPatchRequest { Script = script }, doc.SerializedSizeOnDisk);
						
						return doc;
					}
					catch (ParseException e)
					{
						Log.WarnException(string.Format("Could not parse replication transformation script of '{0}' collection on document {1}", collection, doc.Key), e);

						throw;
					}
					catch (Exception e)
					{
						Log.WarnException(string.Format("Could not apply replication transformation script of '{0}' collection on document {1}", collection, doc.Key), e);

						throw;
					}
				}
			});	
		}
	}
}