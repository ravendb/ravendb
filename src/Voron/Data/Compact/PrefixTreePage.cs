using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    public sealed unsafe class PrefixTreePage
    {
        public readonly byte* Pointer;
        public readonly int PageSize;
        public readonly string Source;

        private PrefixTreePageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (PrefixTreePageHeader*)Pointer; }
        }

        public byte* DataPointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Pointer + sizeof(PrefixTreePageHeader); }
        }

        public PrefixTreePage(byte* pointer, string source, int pageSize)
        {
            Pointer = pointer;
            Source = source;
            PageSize = pageSize;
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->PageNumber = value; }
        }

        public override string ToString()
        {
            return $"#{PageNumber}";
        }

        /// <summary>
        /// Will return the offset from the <see cref="DataPointer"/> to the Node 
        /// </summary>
        /// <param name="nodeNameInTree">the relative name of the node in the tree.</param>
        /// <returns>the offset in the page memory for that node</returns>
        public static long GetNodeOffset(long nodeNameInTree)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Will return a pointer to the actual node in the tree
        /// </summary>
        /// <param name="nodeNameInTree">the relative name of the node in the tree.</param>
        /// <returns>the node for that node</returns>
        public PrefixTree.Node* GetNodeByName(long nodeNameInTree )
        {
            throw new NotImplementedException();
        }
    }
}
