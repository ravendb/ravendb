using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;
using Voron.Impl.FileHeaders;

namespace Voron.Trees.Compact
{
    public unsafe partial class PrefixTree
    {
        private readonly Transaction _tx;
        private readonly PrefixTreeMutableState _state = new PrefixTreeMutableState();

        public PrefixTreeMutableState State
        {
            get { return _state; }
        }        

        private PrefixTree(Transaction tx, long root)
        {
            _tx = tx;
            _state.RootPageNumber = root;
        }

        private PrefixTree(Transaction tx, PrefixTreeMutableState state)
        {
            _tx = tx;
            _state = state;
        }

        public static PrefixTree Open(Transaction tx, TreeRootHeader* header)
        {
            return new PrefixTree(tx, header->RootPageNumber);
        }

        public static PrefixTree Create(Transaction tx, TreeFlags flags = TreeFlags.None)
        {
            throw new NotImplementedException();
            //var newRootPage = tx.AllocatePage(1, TreePageFlags.Leaf);
            
            //var tree = new PrefixTree(tx, newRootPage.PageNumber);
            //tree.State.RecordNewPage(newRootPage, 1);

            //return tree;
        }

        public void Add(Slice key, Stream value, ushort? version = null)
        {
            throw new NotImplementedException();
        }

        public void Add(Slice key, Slice value, ushort? version = null)
        {
            throw new NotImplementedException();
        }

        public void Delete(Slice key, ushort? version = null)
        {
            throw new NotImplementedException();
        }

        public Slice Successor( Slice key )
        {
            throw new NotImplementedException();
        }

        public Slice Predecessor( Slice key )
        {
            throw new NotImplementedException();
        }

        public Slice LastKeyOrDefault()
        {
            throw new NotImplementedException();
        }

        public Slice FirstKeyOrDefault()
        {
            throw new NotImplementedException();
        }
    }
}
