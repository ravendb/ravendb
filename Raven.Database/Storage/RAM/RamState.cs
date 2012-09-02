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
		public TransactionalDictionary<string, TransactionalValue<Attachment>> Attachments { get; set; }
		
		public RamState()
		{
			Attachments = new TransactionalDictionary<string, TransactionalValue<Attachment>>(StringComparer.InvariantCultureIgnoreCase,
					() => new TransactionalValue<Attachment>());


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