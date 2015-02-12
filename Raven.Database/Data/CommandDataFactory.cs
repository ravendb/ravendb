//-----------------------------------------------------------------------
// <copyright file="CommandDataFactory.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Data
{
    public static class CommandDataFactory
    {
        private static readonly char[] IllegalHeaderChars =
        {
            '(', ')', '<', '>', '@'
            , ',', ';', ':', '\\',
            '/', '[', ']', '?', '=',
            '{', '}', (char) 9 /*HT*/, (char) 32 /*SP*/
        };

    public static ICommandData CreateCommand(RavenJObject jsonCommand, TransactionInformation transactionInformation)
		{
			var key = jsonCommand["Key"].Value<string>();
			switch (jsonCommand.Value<string>("Method"))
			{
				case "PUT":
					var putCommand = new PutCommandData
					{
						Key = key,
						Etag = GetEtagFromCommand(jsonCommand),
						Document = jsonCommand["Document"] as RavenJObject,
						Metadata = jsonCommand["Metadata"] as RavenJObject,
						TransactionInformation = transactionInformation
					};
			        ValidateMetadataKeys(putCommand.Metadata);
			        return putCommand;
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
						SkipPatchIfEtagMismatch = jsonCommand.ContainsKey("SkipPatchIfEtagMismatch") && jsonCommand.Value<bool>("SkipPatchIfEtagMismatch")
					};
				case "EVAL":
					var debug = jsonCommand["DebugMode"].Value<bool>();
					return new ScriptedPatchCommandData
					{
						Key = key,
						Metadata = jsonCommand["Metadata"] as RavenJObject,
						Etag = GetEtagFromCommand(jsonCommand),
						TransactionInformation = transactionInformation,
						Patch = ScriptedPatchRequest.FromJson(jsonCommand.Value<RavenJObject>("Patch")),
						PatchIfMissing = jsonCommand["PatchIfMissing"] == null ? null : ScriptedPatchRequest.FromJson(jsonCommand.Value<RavenJObject>("PatchIfMissing")),
						DebugMode = debug
					};
				default:
					throw new ArgumentException("Batching only supports PUT, PATCH, EVAL and DELETE.");
			}
		}

        private static void ValidateMetadataKeys(RavenJObject metaDataProps)
        {
            foreach (var metaDataProp in metaDataProps)
            {
                var key = metaDataProp.Key;
                switch (key[0])
                {
                    case '@':// @ prefix is already handled elsewhere
                        continue;
                    default:
                        foreach (char ch in IllegalHeaderChars)
                        {
                            if (key.IndexOf(ch) == -1)
                                continue;
                            throw new InvalidDataException(string.Format("You aren't allowed to use '{0}' in the metadata", ch));
                        }
                        break;
                }
            }
        }

        private static Etag GetEtagFromCommand(RavenJObject jsonCommand)
		{
			return jsonCommand["Etag"] != null && jsonCommand["Etag"].Value<string>() != null ? Etag.Parse(jsonCommand["Etag"].Value<string>()) : null;
		}
	}
}
