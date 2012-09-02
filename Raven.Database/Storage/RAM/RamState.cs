using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

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

		public RamState()
		{
			AttachmentCount = new TransactionalValue<int>();

			Attachments = new TransactionalDictionary<string, Attachment>(StringComparer.InvariantCultureIgnoreCase);

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
}