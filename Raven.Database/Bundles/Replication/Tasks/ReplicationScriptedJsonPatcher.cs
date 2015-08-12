// -----------------------------------------------------------------------
//  <copyright file="ReplicationScriptedJsonPatcher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Jint;
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Extensions;
using Raven.Database.Json;

namespace Raven.Bundles.Replication.Tasks
{
	internal class ReplicationScriptedJsonPatcher : ScriptedJsonPatcher
	{
		private readonly ScriptedPatchRequest scriptedPatchRequest;

		public ReplicationScriptedJsonPatcher(DocumentDatabase database, ScriptedPatchRequest scriptedPatchRequest)
			: base (database)
		{
			if (string.IsNullOrEmpty(scriptedPatchRequest.Script))
				throw new InvalidOperationException("Patch script must be non-null and not empty");

			this.scriptedPatchRequest = scriptedPatchRequest;
		}

		protected override void CustomizeEngine(Engine engine, ScriptedJsonPatcherOperationScope scope)
		{
			base.CustomizeEngine(engine, scope);

			engine.Execute(string.Format(@"function ExecutePatchScript(docInner){{ return (function(doc){{ {0} }}).apply(docInner); }};", scriptedPatchRequest.Script.NormalizeLineEnding()));
		}
	}
}