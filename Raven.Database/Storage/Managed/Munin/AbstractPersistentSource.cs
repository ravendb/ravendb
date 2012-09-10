//-----------------------------------------------------------------------
// <copyright file="AbstractPersistentSource.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Linq;

namespace Raven.Munin
{
	public abstract class AbstractPersistentSource : IPersistentSource
	{
		private readonly StreamsPool pool;
		private volatile IList<PersistentDictionaryState> globalStates = new List<PersistentDictionaryState>();

		private readonly ThreadLocal<IList<PersistentDictionaryState>> currentStates =
			new ThreadLocal<IList<PersistentDictionaryState>>(() => null);

		private bool disposed;

		public IList<PersistentDictionaryState> CurrentStates
		{
			get { return currentStates.Value; }
			set { currentStates.Value = value; }
			}

		protected AbstractPersistentSource()
		{
			pool = new StreamsPool(CreateClonedStreamForReadOnlyPurposes);
		}

		public bool CreatedNew
		{
			get;
			protected set;
		}

		public IList<PersistentDictionaryState> DictionariesStates
		{
			get
			{
				var persistentDictionaryStates = CurrentStates;
				if(persistentDictionaryStates == null)
					return globalStates;
				return persistentDictionaryStates;
		}
		}

		protected abstract Stream CreateClonedStreamForReadOnlyPurposes();

		public T Read<T>(Func<Stream, T> readOnlyAction)
		{
			if (disposed)
				throw new ObjectDisposedException("Cannot access persistent source after it was disposed");

				Stream stream;
				using (pool.Use(out stream))
					return readOnlyAction(stream);
			}

		public T Read<T>(Func<T> readOnlyAction)
		{
			if(disposed)
				throw new ObjectDisposedException("Cannot access persistent source after it was disposed");
				return readOnlyAction();
			}


		public void Write(Action<Stream> readWriteAction)
		{
			lock (this)
			{
				if (disposed)
					throw new ObjectDisposedException("Cannot access persistent source after it was disposed");
			
				bool success = false;
				try
				{
					CurrentStates = new List<PersistentDictionaryState>(
						globalStates.Select(x => new PersistentDictionaryState(x.Comparer)
						{
							KeyToFilePositionInFiles = x.KeyToFilePositionInFiles,
							SecondaryIndicesState = x.SecondaryIndicesState.ToList()
						}));

					readWriteAction(Log);
					success = true;
				}
				finally
				{
					if(success)
					{
					pool.Clear();
						globalStates = CurrentStates;
					}
					CurrentStates = null;
				}
			}
		}

		protected abstract Stream Log { get; }

		public abstract void ReplaceAtomically(Stream newLog);
		public abstract Stream CreateTemporaryStream();
		public abstract void FlushLog();
		public abstract RemoteManagedStorageState CreateRemoteAppDomainState();

		public void ClearPool()
		{
			pool.Clear();
		}

		public abstract void EnsureCapacity(int value);

		public void BeginTx()
		{
			CurrentStates = globalStates;
		}

		public void CompleteTx()
		{
			CurrentStates = null;
		}

		public virtual void Dispose()
		{
			pool.Dispose();
			currentStates.Dispose();
			disposed = true;
		}
	}
}
