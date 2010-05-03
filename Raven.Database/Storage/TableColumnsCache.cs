using System;
using System.Collections.Generic;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Storage
{
	public class TableColumnsCache
	{
		public IDictionary<string, JET_COLUMNID> DocumentsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> TasksColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> FilesColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> IndexesStatsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> MappedResultsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> DocumentsModifiedByTransactionsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> TransactionsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> IdentityColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> DetailsColumns { get; set; }
	}
}