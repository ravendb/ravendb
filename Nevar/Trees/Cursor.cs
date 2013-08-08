using System;
using System.Collections.Generic;
using System.Linq;
using Nevar.Debugging;
using Nevar.Impl;

namespace Nevar.Trees
{
    public class Cursor : IDisposable
    {
        private readonly Tree _tree;
        private readonly Transaction _transaction;
        public LinkedList<Page> Pages = new LinkedList<Page>();

        public Cursor(Tree tree, Transaction transaction)
        {
            _tree = tree;
            _transaction = transaction;
        }

        public Page ParentPage
        {
            get
            {
                LinkedListNode<Page> linkedListNode = Pages.First;
                if (linkedListNode == null)
                    throw new InvalidOperationException("No pages in cursor");
                linkedListNode = linkedListNode.Next;
                if (linkedListNode == null)
                    throw new InvalidOperationException("No parent page in cursor");
                return linkedListNode.Value;
            }
        }

        public Page CurrentPage
        {
            get
            {
                LinkedListNode<Page> linkedListNode = Pages.First;
                if (linkedListNode == null)
                    throw new InvalidOperationException("No pages in cursor");
                return linkedListNode.Value;
            }
        }

        public void Push(Page p)
        {
#if DEBUG
            // make sure that we aren't already there
            if (Pages.Any(page => page.PageNumber == p.PageNumber))
            {
                throw new InvalidOperationException("Pushed page already in cursor's stack: " + p.PageNumber);
            }
#endif
            Pages.AddFirst(p);
        }

        public Page Pop()
        {
            if (Pages.Count == 0)
            {
                throw new InvalidOperationException("No page to pop");
            }
            Page p = Pages.First.Value;
            Pages.RemoveFirst();
            return p;
        }

	    public Page GetPage(long p)
	    {
		    return Pages.FirstOrDefault(x => x.PageNumber == p);
	    }

        public void Dispose()
        {
            _transaction.RemoveCursor(_tree, this);
        }
    }
}