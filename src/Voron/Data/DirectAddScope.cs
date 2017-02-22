using System;
using Voron.Data.BTrees;

namespace Voron.Data
{
    public unsafe class DirectAddScope : IDisposable
    {
        private readonly object _tree;
        private uint _usage;
#if DEBUG
        private string _allocationStacktrace = null;
#endif
        public byte* Ptr;

        public DirectAddScope(object tree)
        {
            _usage = 0;
            _tree = tree;
            Ptr = null;
        }
        
        public DirectAddScope Open(byte* writePos)
        {
            if (_usage++ <= 0)
            {
                Ptr = writePos;
#if VALIDATE_DIRECT_ADD_STACKTRACE
                _allocationStacktrace = Environment.StackTrace;
#endif
            }
            else
            {
#if DEBUG
                ThrowScopeAlreadyOpen(_tree, _allocationStacktrace);
#else
                ThrowScopeAlreadyOpen(_tree, null);
#endif
            }

            return this;
        }

        private static void ThrowScopeAlreadyOpen(object tree, string previousOpenStacktrace)
        {
            var message = $"Write operation already requested on a tree name: {tree}. " +
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

        public void Reset()
        {
            if (_usage <= 0)
                return;

            ThrowScopeAlreadyOpen(_tree, null);
        }
    }
}