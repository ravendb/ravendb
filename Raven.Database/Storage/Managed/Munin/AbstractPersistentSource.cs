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
		private volatile IList<PersistentDictionaryState> globalStates = new List<PersistentDictionaryState>();

        private readonly ThreadLocal<Stack<IList<PersistentDictionaryState>>> currentStates =
            new ThreadLocal<Stack<IList<PersistentDictionaryState>>>(() => new Stack<IList<PersistentDictionaryState>>());

		private bool disposed;

		public IList<PersistentDictionaryState> CurrentStates
		{
			get
			{
			    if (currentStates.Value.Count == 0)
			        return null;
			    return currentStates.Value.Peek();
			}
			set
			{
                if (value == null)
                {
                    if (currentStates.Value.Count > 0)
                        currentStates.Value.Pop();
                }
                else
                {
                    currentStates.Value.Push(value);
                }
			}
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
				if (persistentDictionaryStates == null)
					return globalStates;
				return persistentDictionaryStates;
			}
		}

		protected abstract Stream CreateClonedStreamForReadOnlyPurposes();

		public T Read<T>(Func<Stream, T> readOnlyAction)
		{
			if (disposed)
				throw new ObjectDisposedException("Cannot access persistent source after it was disposed");

			using (var stream = CreateClonedStreamForReadOnlyPurposes())
				return readOnlyAction(stream);
		}

		public T Read<T>(Func<T> readOnlyAction)
		{
			if (disposed)
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
					if (CurrentStates == null)
					{
						CurrentStates = new List<PersistentDictionaryState>(
							globalStates.Select(x => new PersistentDictionaryState(x.Comparer)
													 {
														 KeyToFilePositionInFiles = x.KeyToFilePositionInFiles,
														 SecondaryIndicesState = x.SecondaryIndicesState.ToList()
													 }));
					}

					readWriteAction(Log);
					success = true;
				}
				finally
				{
					if (success)
					{
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

		public abstract void EnsureCapacity(int value);

		public void BeginTx()
		{
			lock (this)
			{
				CurrentStates = globalStates;
			}
		}

		public void CompleteTx()
		{
			CurrentStates = null;
		}

		public virtual void Dispose()
		{
			currentStates.Dispose();
			disposed = true;
		}
	}
}
