//-----------------------------------------------------------------------
// <copyright file="TableStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Json.Linq;
using Raven.Munin;

namespace Raven.Storage.Managed.Impl
{
	public class TableStorage : Munin.Database
	{
		public TableStorage(IPersistentSource persistentSource)
			: base(persistentSource)
		{
			Details = Add(new Table("Details"));

			Identity = Add(new Table(x => x.Value<string>("name"), "Identity"));

			Attachments = Add(new Table(x => x.Value<string>("key"), "Attachments")
			{
				{"ByKey", x => x.Value<string>("key")},
				{"ByEtag", x => new ComparableByteArray(x.Value<byte[]>("etag"))},
			});

			Documents = Add(new Table(x => x.Value<string>("key"), "Documents")
			{
				{"ByKey", x => x.Value<string>("key")},
				{"ById", x => x.Value<string>("id")},
				{"ByEtag", x => new ComparableByteArray(x.Value<byte[]>("etag"))}
			});

			DocumentsModifiedByTransactions =
				Add(new Table(x => new RavenJObject{{"key", x.Value<string>("key")}},
							  "DocumentsModifiedByTransactions")
				{
					{"ByTxId", x => new ComparableByteArray(x.Value<byte[]>("txId"))}
				});

			Transactions =
				Add(new Table(x => x.Value<byte[]>("txId"), "Transactions"));

			IndexingStats =
				Add(new Table(x => x.Value<string>("index"), "IndexingStats"));


			MappedResults = Add(new Table("MappedResults")
			{
				{"ByViewAndReduceKey", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("reduceKey"))},
				{"ByViewAndDocumentId", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("docId"))},
				{"ByViewAndEtagDesc", x => Tuple.Create(x.Value<string>("view"), new ReverseComparableByteArrayWhichIgnoresNull(x.Value<byte[]>("etag")))},
				{"ByViewAndEtag", x => Tuple.Create(x.Value<string>("view"), new ComparableByteArray(x.Value<byte[]>("etag")))}
	
			});

			Queues = Add(new Table(x => new RavenJObject
			{
				{"name", x.Value<string>("name")},
				{"id", x.Value<byte[]>("id")}
			}, "Queues")
			{
				{"ByName", x => x.Value<string>("name")}
			});

			Tasks = Add(new Table(x => new RavenJObject
			{
				{"index", x.Value<string>("index")},
				{"id", x.Value<byte[]>("id")}
			}, "Tasks")
			{
				{"ByIndexAndTime", x => Tuple.Create(x.Value<string>("index"), x.Value<DateTime>("time"))},
				{"ByIndexAndType", x => Tuple.Create(x.Value<string>("index"), x.Value<string>("type"))}
			});
		}

		public Table Details { get; private set; }

		public Table Tasks { get; private set; }

		public Table Queues { get; private set; }

		public Table MappedResults { get; private set; }

		public Table IndexingStats { get; private set; }

		public Table Transactions { get; private set; }

		public Table DocumentsModifiedByTransactions { get; private set; }

		public Table Documents { get; private set; }

		public Table Attachments { get; private set; }

		public Table Identity { get; private set; }

		public override void Dispose()
		{
			base.Dispose();
		}
	}
}