using System;
using Voron.Data.BTrees;

namespace Voron.Data
{
    public unsafe class DirectAddScope : IDisposable
    {
        private readonly string _treeName;
        private uint _usage;
#if DEBUG
        private string _allocationStacktrace = null;
#endif
        public byte* Ptr;

        public DirectAddScope(string treeName)
        {
            _usage = 0;
            _treeName = treeName;
            Ptr = null;
        }

        public DirectAddScope Open(byte* writePos)
        {
            if (_usage++ <= 0)
            {
                Ptr = writePos;
#if DEBUG
                // uncomment for debugging purposes only
                //_allocationStacktrace = Environment.StackTrace;
#endif
            }
            else
            {
#if DEBUG
                ThrowScopeAlreadyOpen(_treeName, _allocationStacktrace);
#else
                ThrowScopeAlreadyOpen(_treeName, null);
#endif
            }

            return this;
        }

        private static void ThrowScopeAlreadyOpen(string treeName, string previousOpenStacktrace)
        {
            var message = $"Write operation already requested on a tree name: {treeName}. " +
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

            ThrowScopeAlreadyOpen(_treeName, null);
        }
    }
}