//-----------------------------------------------------------------------
// <copyright file="CommandExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Commands;
using Raven.Database.Data;

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
				database.ApplyPatch(advPatchCommandData.Key, advPatchCommandData.Etag,
									advPatchCommandData.Patch, advPatchCommandData.TransactionInformation);

				var doc = database.Get(advPatchCommandData.Key, advPatchCommandData.TransactionInformation);
				if (doc != null)
				{
					advPatchCommandData.Metadata = doc.Metadata;
					advPatchCommandData.Etag = doc.Etag;
				}
				return;
			}
		}
	}
}