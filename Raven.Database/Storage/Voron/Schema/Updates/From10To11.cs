// -----------------------------------------------------------------------
//  <copyright file="From10To11.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage.Voron.Impl;
using Raven.Json.Linq;
using Voron;

namespace Raven.Database.Storage.Voron.Schema.Updates
{
	public class From10To11 : SchemaUpdateBase
	{
		public override string FromSchemaVersion
		{
			get { return "1.0"; }
		}
		public override string ToSchemaVersion
		{
			get { return "1.1"; }
		}

		public override void Update(TableStorage tableStorage, Action<string> output)
		{
			using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var lists = tx.ReadTree(Tables.Lists.TableName);

				var iterator = lists.Iterate();

				if (iterator.Seek(Slice.BeforeAllKeys))
				{
					do
					{
						var result = lists.Read(iterator.CurrentKey);

						using (var stream = result.Reader.AsStream())
						{
							var listItem = stream.ToJObject();

							if (listItem.ContainsKey("createdAt"))
								continue;
							
							listItem.Add("createdAt", SystemTime.UtcNow);

							using (var streamValue = new MemoryStream())
							{
								listItem.WriteTo(streamValue);
								streamValue.Position = 0;

								lists.Add(iterator.CurrentKey, streamValue);
							}
						}

					} while (iterator.MoveNext());
				}

				tx.Commit();
			}

			UpdateSchemaVersion(tableStorage, output);
		}
	}
}