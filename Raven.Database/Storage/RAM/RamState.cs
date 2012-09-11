using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;

namespace Raven.Database.Storage.RAM
{
	public class RamState
	{
		public TransactionalDictionary<string, TransactionalDictionary<string, ListItem>> Lists { get; set; }
		public TransactionalDictionary<string, TransactionalDictionary<Guid, byte[]>> Queues { get; set; }
		public TransactionalDictionary<string, TransactionalDictionary<Guid, byte[]>> Tasks { get; set; }
		public TransactionalDictionary<string, TransactionalValue<long>> Identities { get; set; }
		public TransactionalDictionary<string, Attachment> Attachments { get; set; }
		public TransactionalValue<int> AttachmentCount { get; set; }
		public TransactionalDictionary<string, DocuementWrapper> Documents { get; set; }
		public TransactionalDictionary<string, DocumentsModifiedByTransation> DocumentsModifiedByTransations { get; set; }
		public TransactionalValue<long> DocumentCount { get; set; }
		public TransactionalDictionary<Guid, Transaction> Transactions { get;set; }
		public TransactionalDictionary<string, Index> Indexes { get; set; }
		public TransactionalDictionary<string, IndexStats> IndexesStats { get; set; }
		public TransactionalDictionary<string,TransactionalList<MappedResultsWrapper>> MappedResults { get; set; }
		public TransactionalDictionary<string, TransactionalList<ScheduledReductionInfo>> ScheduledReductions { get; set; } 

		public RamState()
		{
			AttachmentCount = new TransactionalValue<int>();

			Attachments = new TransactionalDictionary<string, Attachment>(StringComparer.InvariantCultureIgnoreCase);

			DocumentCount = new TransactionalValue<long>();

			Documents = new  TransactionalDictionary<string, DocuementWrapper>(StringComparer.InvariantCultureIgnoreCase);

			Transactions = new TransactionalDictionary<Guid, Transaction>(EqualityComparer<Guid>.Default);

			Indexes = new TransactionalDictionary<string, Index>(StringComparer.InvariantCultureIgnoreCase);

			IndexesStats = new TransactionalDictionary<string, IndexStats>(StringComparer.InvariantCultureIgnoreCase);

			MappedResults = new TransactionalDictionary<string, TransactionalList<MappedResultsWrapper>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalList<MappedResultsWrapper>());

			ScheduledReductions = new TransactionalDictionary<string, TransactionalList<ScheduledReductionInfo>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalList<ScheduledReductionInfo>());

			Lists = new TransactionalDictionary<string, TransactionalDictionary<string, ListItem>>(StringComparer.InvariantCultureIgnoreCase,
					() => new TransactionalDictionary<string, ListItem>(StringComparer.InvariantCultureIgnoreCase));

			Queues = new TransactionalDictionary<string, TransactionalDictionary<Guid, byte[]>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalDictionary<Guid, byte[]>(EqualityComparer<Guid>.Default));

			Tasks = new TransactionalDictionary<string, TransactionalDictionary<Guid, byte[]>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalDictionary<Guid, byte[]>(EqualityComparer<Guid>.Default));

			Identities = new TransactionalDictionary<string, TransactionalValue<long>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalValue<long>{Value = 0L});
		}
	}

	public class Transaction
	{
		public Guid Key { get; set; }
		public DateTime TimeOut { get; set; }
	}

	public class DocuementWrapper
	{
		public JsonDocument Document { get; set; }
		public Guid? LockByTransaction { get; set; }
	}

	public class DocumentsModifiedByTransation
	{
		public JsonDocument Document { get; set; }
		public Guid? LockByTransaction { get; set; }
		public bool DeleteDocument { get; set; }
	}

	public class MappedResultsWrapper
	{
		public MappedResultInfo MappedResultInfo { get; set; }
		public string View { get; set; }
		public string DocumentKey { get; set; }
	}
}