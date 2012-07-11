//-----------------------------------------------------------------------
// <copyright file="AbstractPersistentSource.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Linq;

namespace Raven.Munin
{
	public abstract class AbstractPersistentSource : IPersistentSource
	{
		private readonly StreamsPool pool;
		private IList<PersistentDictionaryState> globalStates = new List<PersistentDictionaryState>();

		private readonly ThreadLocal<IList<PersistentDictionaryState>> currentStates =
			new ThreadLocal<IList<PersistentDictionaryState>>(() => null);

		private bool disposed;

		public IList<PersistentDictionaryState> CurrentStates
		{
			get { return currentStates.Value; }
			set
			{
				currentStates.Value = value;
			}
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
			get { return CurrentStates ?? globalStates; }
		}

		protected abstract Stream CreateClonedStreamForReadOnlyPurposes();

		public T Read<T>(Func<Stream, T> readOnlyAction)
		{
			if (disposed)
				throw new ObjectDisposedException("Cannot access persistent source after it was disposed");

			var oldValue = CurrentStates;
			CurrentStates = oldValue ?? globalStates;
			try
			{
				Stream stream;
				using (pool.Use(out stream))
					return readOnlyAction(stream);
			}
			finally
			{
				CurrentStates = oldValue;
			}
		}

		public T Read<T>(Func<T> readOnlyAction)
		{
			if(disposed)
				throw new ObjectDisposedException("Cannot access persistent source after it was disposed");
			var oldValue = CurrentStates;
			CurrentStates = oldValue ?? globalStates;
			try
			{
				return readOnlyAction();
			}
			finally
			{
				CurrentStates = oldValue;
			}
		}


		public void Write(Action<Stream> readWriteAction)
		{
			lock (this)
			{
				if (disposed)
					throw new ObjectDisposedException("Cannot access persistent source after it was disposed");
			
				try
				{
					CurrentStates = new List<PersistentDictionaryState>(
						globalStates.Select(x => new PersistentDictionaryState(x.Comparer)
						{
							KeyToFilePositionInFiles = x.KeyToFilePositionInFiles,
							SecondaryIndicesState = x.SecondaryIndicesState.ToList()
						}));

					readWriteAction(Log);
				}
				finally
				{
					pool.Clear();
					Interlocked.Exchange(ref globalStates, CurrentStates);
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

		public virtual void Dispose()
		{
			pool.Dispose();
			currentStates.Dispose();
			disposed = true;
		}
	}
}