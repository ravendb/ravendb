//-----------------------------------------------------------------------
// <copyright file="IQueueStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Database.Storage
{
	public interface IQueueStorageActions
	{
		void EnqueueToQueue(string name, byte[] data);
		IEnumerable<Tuple<byte[], object>> PeekFromQueue(string name);
		void DeleteFromQueue(string name, object id);
	}
}
