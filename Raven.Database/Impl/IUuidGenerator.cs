//-----------------------------------------------------------------------
// <copyright file="IUuidGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Database.Impl
{
	public interface IUuidGenerator
	{
		Guid CreateSequentialUuid(UuidType type);
	}

	public enum UuidType : byte
	{
		Documents = 1,
		Attachments = 2,
		DocumentTransactions = 3,
		MappedResults = 4,
		ReduceResults = 5,
		ScheduledReductions = 6,
		Queue = 7,
		Tasks = 8,
		Indexing = 9
	}
}
