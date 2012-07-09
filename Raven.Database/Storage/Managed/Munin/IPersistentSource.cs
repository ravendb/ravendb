//-----------------------------------------------------------------------
// <copyright file="IPersistentSource.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Munin
{
	public interface IPersistentSource : IDisposable
	{
		T Read<T>(Func<Stream,T> readOnlyAction);

		T Read<T>(Func<T> readOnlyAction);

		void Write(Action<Stream> readWriteAction);

		bool CreatedNew { get; }
		IList<PersistentDictionaryState> DictionariesStates { get; }

		void ReplaceAtomically(Stream newLog);

		Stream CreateTemporaryStream();

		void FlushLog();
		RemoteManagedStorageState CreateRemoteAppDomainState();
		void ClearPool();
		void EnsureCapacity(int value);
	}
}