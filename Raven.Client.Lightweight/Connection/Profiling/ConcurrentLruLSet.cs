using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Raven.Client.Connection.Profiling
{
	internal class ConcurrentLruLSet<T>
	{
		private readonly int maxCapacity;
		private readonly Action<T> onDrop;
        private readonly ReaderWriterLockSlim rwLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

		private LinkedList<T> items = new LinkedList<T>();        

		public ConcurrentLruLSet(int maxCapacity, Action<T> onDrop = null)
		{
			this.maxCapacity = maxCapacity;
			this.onDrop = onDrop;
		}

		public T FirstOrDefault(Func<T, bool> predicate)
		{
            try
            {
                rwLock.EnterReadLock();

                return items.FirstOrDefault(predicate);
            }
            finally
            {
                rwLock.ExitReadLock();
            }		
		}

		public void Push(T item)
		{
            LinkedListNode<T> linkedListNode = null;

            try
            {
                rwLock.EnterWriteLock();


                // this ensures the item is at the head of the list
                items.Remove(item);
                items.AddLast(item);

                if (items.Count > maxCapacity)
                {
                    linkedListNode = items.First;
                    items.RemoveFirst();
                }
            }
            finally
            {
                rwLock.ExitWriteLock();
            }

            if (onDrop != null && linkedListNode != null)
                onDrop(linkedListNode.Value);
		}

		public void Clear()
		{
            // WARNING: We can acquire a read lock and still 'write' because readers wont care (because of what they do)
            // to get a different reference, however this may not hold true if the logic changes.

            try
            {
                rwLock.EnterReadLock();

                items = new LinkedList<T>();
            }
            finally
            {
                rwLock.ExitReadLock();
            }
		}

		public void ClearHalf()
		{
            // WARNING: We can acquire a read lock and still 'write' because readers wont care (because of what they do)
            // to get a different reference, however this may not hold true if the logic changes.

            LinkedList<T> current;            
            try
            {
                rwLock.EnterReadLock();

                current = items;

                items = new LinkedList<T>(current.Skip(current.Count / 2));
            }
            finally
            {
                rwLock.ExitReadLock();
            }

            if (onDrop != null)
            {
                foreach (var item in current.Take(current.Count / 2))
                    onDrop(item);
            }
		}

		public void Remove(T val)
		{
            try
            {
                rwLock.EnterWriteLock();

                items.Remove(val);
            }
            finally
            {
                rwLock.ExitWriteLock();
            }
		}
	}
}