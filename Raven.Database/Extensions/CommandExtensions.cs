//-----------------------------------------------------------------------
// <copyright file="CommandExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Extensions
{
	public static class CommandExtensions
	{
		public static void Execute(this ICommandData self, DocumentDatabase database)
		{
			var deleteCommandData = self as DeleteCommandData;
			if (deleteCommandData != null)
			{
				database.Delete(deleteCommandData.Key, deleteCommandData.Etag, deleteCommandData.TransactionInformation);
				return;
			}

			var putCommandData = self as PutCommandData;
			if (putCommandData != null)
			{
				var putResult = database.Put(putCommandData.Key, putCommandData.Etag, putCommandData.Document, putCommandData.Metadata, putCommandData.TransactionInformation);
				putCommandData.Etag = putResult.ETag;
				putCommandData.Key = putResult.Key;

				return;
			}

			var patchCommandData = self as PatchCommandData;
			if (patchCommandData != null)
			{
				database.ApplyPatch(patchCommandData.Key, patchCommandData.Etag, patchCommandData.Patches, patchCommandData.TransactionInformation);

				var doc = database.Get(patchCommandData.Key, patchCommandData.TransactionInformation);
				if (doc != null)
				{
					patchCommandData.Metadata = doc.Metadata;
					patchCommandData.Etag = doc.Etag;
				}
				return;
			}

			var advPatchCommandData = self as ScriptedPatchCommandData;
			if (advPatchCommandData != null)
			{
				var result = database.ApplyPatch(advPatchCommandData.Key, advPatchCommandData.Etag,
									advPatchCommandData.Patch, advPatchCommandData.TransactionInformation, advPatchCommandData.DebugMode);

				advPatchCommandData.AdditionalData = new RavenJObject { { "Debug", new RavenJArray(result.Item2) } };
				if(advPatchCommandData.DebugMode)
				{
					advPatchCommandData.AdditionalData["Document"] = result.Item1.Document;
					return;
				}

				var doc = database.Get(advPatchCommandData.Key, advPatchCommandData.TransactionInformation);
				if (doc != null)
				{
					advPatchCommandData.Metadata = doc.Metadata;
					advPatchCommandData.Etag = doc.Etag;
				}
				return;
			}

			var patchOrPutCommandData = self as PatchOrPutCommandData;
			if (patchOrPutCommandData != null)
			{
				var result = database.ApplyPatch(patchOrPutCommandData.Key, patchOrPutCommandData.Etag,
												 patchOrPutCommandData.Patches, patchOrPutCommandData.TransactionInformation);

				if (result.PatchResult != PatchResult.DocumentDoesNotExists)
				{
					var doc = database.Get(patchOrPutCommandData.Key, patchOrPutCommandData.TransactionInformation);
					if (doc != null)
					{
						patchOrPutCommandData.Metadata = doc.Metadata;
						patchOrPutCommandData.Etag = doc.Etag;
					}
				}
				else
				{
					var putResult = database.Put(patchOrPutCommandData.Key, patchOrPutCommandData.Etag,
												 patchOrPutCommandData.Document, patchOrPutCommandData.Metadata,
												 patchOrPutCommandData.TransactionInformation);
					patchOrPutCommandData.Etag = putResult.ETag;
					patchOrPutCommandData.Key = putResult.Key;
				}

				return;
			}
		}
	}
}