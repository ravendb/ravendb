// -----------------------------------------------------------------------
//  <copyright file="StorageActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.StorageActions
{
	using System;

	using Raven.Abstractions.Extensions;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;

	public abstract class StorageActionsBase
	{
		protected SnapshotReader Snapshot { get; private set; }

		protected StorageActionsBase(SnapshotReader snapshot)
		{
			this.Snapshot = snapshot;
		}

		protected string CreateKey(params object[] values)
		{
			if (values == null || values.Length == 0)
				throw new InvalidOperationException("Cannot create an empty key.");

			var key = string.Empty;
			for (var i = 0; i < values.Length; i++)
			{
				var value = values[i];
				key += value;
				if (i < values.Length - 1)
					key += "/";
			}

			return key;
		}

		protected RavenJObject LoadJson(Table table, Slice key, out ushort version)
		{
			using (var read = table.Read(this.Snapshot, key))
			{
				if (read == null)
				{
					version = 0;
					return null;
				}

				version = read.Version;
				return read.Stream.ToJObject();
			}
		}
	}
}