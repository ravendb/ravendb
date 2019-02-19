using System;
using System.Collections.Generic;
using System.Linq;

namespace Sparrow.Server.Collections
{
    public class ConcurrentLruSet<T>
    {
        private readonly int maxCapacity;
        private readonly Action<T> onDrop;
        private readonly Action<T> onInsert;
        private readonly object syncRoot = new object();

        private LinkedList<T> items = new LinkedList<T>();
        private Dictionary<T, LinkedListNode<T>> itemsLookupTable = new Dictionary<T, LinkedListNode<T>>();

        public ConcurrentLruSet(int maxCapacity, Action<T> onDrop = null, Action<T> onInsert = null)
        {
            this.maxCapacity = maxCapacity;
            this.onDrop = onDrop;
            this.onInsert = onInsert;
        }

        public T FirstOrDefault(Func<T, bool> predicate)
        {
            lock (syncRoot)
            {
                return items.FirstOrDefault(predicate);
            }
        }

        public void Push(T item)
        {
            LinkedListNode<T> droppedNode = null;

            lock (syncRoot)
            {

                LinkedListNode<T> node;
                if (itemsLookupTable.TryGetValue(item, out node))
                {
                    // this ensures the item is at the head of the list
                    items.Remove(node);
                    items.AddLast(node);
                }
                else
                {
                    node = items.AddLast(item);
                    itemsLookupTable[item] = node;
                }

                if (items.Count > maxCapacity)
                {
                    droppedNode = items.First;
                    
                    items.RemoveFirst();
                    itemsLookupTable.Remove(droppedNode.Value);
                }

                onInsert?.Invoke(item);
            }

            if (onDrop != null && droppedNode != null)
                onDrop(droppedNode.Value);
        }

        public void Clear()
        {
            lock (syncRoot)
            {
                items = new LinkedList<T>();
                itemsLookupTable.Clear();
            }
        }

        public void ClearHalf()
        {
            LinkedList<T> current;
            lock (syncRoot)
            {
                current = items;

                items = new LinkedList<T>();
                itemsLookupTable.Clear();

                foreach ( var item in current.Skip(current.Count / 2))
                {
                    var node = items.AddLast(item);
                    itemsLookupTable[item] = node;
                }
            }

            if (onDrop != null)
            {
                foreach (var item in current.Take(current.Count / 2))
                    onDrop(item);
            }
        }

        public void Remove(T item)
        {
            lock (syncRoot)
            {
                var node = itemsLookupTable[item];

                items.Remove(node);
                itemsLookupTable.Remove(item);
            }
        }
    }
}
