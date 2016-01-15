using System.Runtime.InteropServices;

namespace Voron.Data.Compact
{
    //TODO: Change this when we are ready to go.
    //[StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential)]
    public struct PrefixTreeRootHeader
    {
        /// <summary>
        /// The table header page for the tree.
        /// </summary>
        public long Table;

        /// <summary>
        /// This is the amount of elements already stored in the tree. 
        /// </summary>
        public long Items;

        /// <summary>
        /// This is the translation table header. 
        /// </summary>
        public PrefixTreeTranslationTableHeader TranslationTable;

        /// <summary>
        /// The head node pointer for the tree. 
        /// </summary>
        public PrefixTree.Leaf Head;

        /// <summary>
        /// The tail node pointer for the tree. 
        /// </summary>
        public PrefixTree.Leaf Tail;
    }
}
