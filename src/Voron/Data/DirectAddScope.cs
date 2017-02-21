using System;
using Voron.Data.BTrees;

namespace Voron.Data
{
    public unsafe class DirectAddScope : IDisposable
    {
        private readonly ITree _tree;
        private uint _usage;
        private string _allocationStacktrace = null;

        public byte* Ptr;

        public DirectAddScope(ITree tree)
        {
            _usage = 0;
            _tree = tree;
            Ptr = null;
        }

        public DirectAddScope Open(byte* writePos)
        {
            if (_usage++ > 0)
                ThrowScopeAlreadyOpen(_tree, _allocationStacktrace);

            Ptr = writePos;
#if DEBUG
            // uncomment for debugging purposes only
            //_allocationStacktrace = Environment.StackTrace;
#endif
            return this;
        }

        private static void ThrowScopeAlreadyOpen(ITree tree, string previousOpenStacktrace)
        {
            var message = $"Write operation already requested on a tree name: {tree.Name}. " +
                          $"{nameof(Tree.DirectAdd)} method cannot be called recursively while the scope is already opened.";

            if (previousOpenStacktrace != null)
            {
                message += $"{Environment.NewLine}Stacktrace of previous {nameof(DirectAddScope)}:" +
                           $"{Environment.NewLine}{previousOpenStacktrace}{Environment.NewLine} --- end of {nameof(DirectAddScope)} allocation ---{Environment.NewLine}";
            }

            throw new InvalidOperationException(message);
        }

        public void Dispose()
        {
            _usage--;
            Ptr = null;
        }
    }
}