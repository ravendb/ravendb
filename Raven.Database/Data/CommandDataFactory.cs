//-----------------------------------------------------------------------
// <copyright file="CommandDataFactory.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Data
{
	public static class CommandDataFactory
	{
		public static ICommandData CreateCommand(RavenJObject jsonCommand, TransactionInformation transactionInformation)
		{
			string key = String.Empty;
			if (jsonCommand.ContainsKey("Key"))
			    key = jsonCommand["Key"].Value<string>();
			    
			switch (jsonCommand.Value<string>("Method"))
			{
				case "PUT":
					return new PutCommandData
					{
						Key = key,
						Etag = GetEtagFromCommand(jsonCommand),
						Document = jsonCommand["Document"] as RavenJObject,
						Metadata = jsonCommand["Metadata"] as RavenJObject,
						TransactionInformation = transactionInformation
					};
				case "DELETE":
					return new DeleteCommandData
					{
						Key = key,
						Etag = GetEtagFromCommand(jsonCommand),
						TransactionInformation = transactionInformation
					};
				case "PATCH":
					return new PatchCommandData
					{
						Key = key,
						Etag = GetEtagFromCommand(jsonCommand),
						TransactionInformation = transactionInformation,
						Metadata = jsonCommand["Metadata"] as RavenJObject,
						Patches = jsonCommand
							.Value<RavenJArray>("Patches")
							.Cast<RavenJObject>()
							.Select(PatchRequest.FromJson)
							.ToArray(),
						PatchesIfMissing = jsonCommand["PatchesIfMissing"] == null ? null : jsonCommand
							.Value<RavenJArray>("PatchesIfMissing")
							.Cast<RavenJObject>()
							.Select(PatchRequest.FromJson)
							.ToArray(),
					};
				case "EVAL":
					var debug = jsonCommand["DebugMode"].Value<bool>();
					return new ScriptedPatchCommandData
					{
						Key = key,
						Etag = GetEtagFromCommand(jsonCommand),
						Metadata = jsonCommand["Metadata"] as RavenJObject,
						TransactionInformation = transactionInformation,
						Patch = ScriptedPatchRequest.FromJson(jsonCommand.Value<RavenJObject>("Patch")),
						PatchIfMissing = jsonCommand["PatchIfMissing"] == null ? null : ScriptedPatchRequest.FromJson(jsonCommand.Value<RavenJObject>("PatchIfMissing")),
						DebugMode = debug
					};
				default:
					throw new ArgumentException("Batching only supports PUT, PATCH, EVAL and DELETE.");
			}
		}

		private static Etag GetEtagFromCommand(RavenJObject jsonCommand)
		{
			return jsonCommand["Etag"] != null && jsonCommand["Etag"].Value<string>() != null ? Etag.Parse(jsonCommand["Etag"].Value<string>()) : null;
		}
	}
}
