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
			var key = jsonCommand["Key"].Value<string>();
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
						Patches = jsonCommand
							.Value<RavenJArray>("Patches")
							.Cast<RavenJObject>()
							.Select(PatchRequest.FromJson)
							.ToArray()
					};
				case "EVAL":
					return new ScriptedPatchCommandData
					{
						Key = key,
						Etag = GetEtagFromCommand(jsonCommand),
						TransactionInformation = transactionInformation,
						Patch = ScriptedPatchRequest.FromJson(jsonCommand.Value<RavenJObject>("Patch"))
					};
				default:
					throw new ArgumentException("Batching only supports PUT, PATCH, EVAL and DELETE.");
			}
		}

		private static Guid? GetEtagFromCommand(RavenJObject jsonCommand)
		{
			return jsonCommand["Etag"] != null && jsonCommand["Etag"].Value<string>() != null ? new Guid(jsonCommand["Etag"].Value<string>()) : (Guid?)null;
		}
	}
}
