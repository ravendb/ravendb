// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerStreamTransformerActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Impl.Streams
{
	public class DatabaseSmugglerStreamTransformerActions : DatabaseSmugglerStreamActionsBase, IDatabaseSmugglerTransformerActions
	{
		public DatabaseSmugglerStreamTransformerActions(JsonTextWriter writer)
			: base(writer, "Transformers")
		{
		}

		public Task WriteTransformerAsync(TransformerDefinition transformer, CancellationToken cancellationToken)
		{
			var transformerJson = RavenJObject.FromObject(transformer);
			transformerJson.WriteTo(Writer);
			return new CompletedTask();
		}
	}
}