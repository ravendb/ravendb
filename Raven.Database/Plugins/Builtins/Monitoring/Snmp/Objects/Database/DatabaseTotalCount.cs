// -----------------------------------------------------------------------
//  <copyright file="DatabaseTotalCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;

using Lextm.SharpSnmpLib;

using Raven.Abstractions.Data;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database
{
	public class DatabaseTotalCount : ScalarObjectBase<Integer32>
	{
		private readonly DocumentDatabase systemDatabase;

		public DatabaseTotalCount(DocumentDatabase systemDatabase)
			: base("5.1.1")
		{
			this.systemDatabase = systemDatabase;
		}

		protected override Integer32 GetData()
		{
			return new Integer32(GetCount(systemDatabase));
		}

		private static int GetCount(DocumentDatabase database)
		{
			var nextStart = 0;
			var documents = database.Documents.GetDocumentsWithIdStartingWith(Constants.Database.Prefix, null, null, 0, int.MaxValue, CancellationToken.None, ref nextStart);

			return documents.Length;
		}
	}
}