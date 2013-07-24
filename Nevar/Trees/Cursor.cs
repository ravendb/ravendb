using System;
using System.Collections.Generic;

namespace Nevar.Trees
{
    public class Cursor
    {
        public LinkedList<Page> Pages = new LinkedList<Page>();

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
    }
}