// -----------------------------------------------------------------------
//  <copyright file="VoronStorageStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Voron.Debugging;

namespace Raven.Database.Storage.Voron
{
	public class VoronStorageStats : StorageStats
	{
		public long FreePagesOverhead;
		public long RootPages;
		public long UnallocatedPagesAtEndOfFile;
		public long UsedDataFileSizeInBytes;
		public long AllocatedDataFileSizeInBytes;
		public long NextWriteTransactionId;
		public List<ActiveTransaction> ActiveTransactions;
	}
}