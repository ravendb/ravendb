using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sparrow.Threading
{
    /// <summary>
    /// An atomic, thread-safe, multiple use flag that can be raised and
    /// lowered at will; meant to be single-user. DO NOT PASS THIS AROUND.
    /// </summary>
    /// 
    /// Example use case is one class has multiple threads that access it
    /// and change the behavior externally, such as "stop logging".
    /// 
    /// For convincing on why you should use this class instead of rolling
    /// your own, see
    /// http://blog.alexrp.com/2014/03/30/dot-net-atomics-and-memory-model-semantics/
    /// and http://issues.hibernatingrhinos.com/issue/RavenDB-8260 .
    /// 
    /// PERF: This is a class instead of a struct simply because we can not
    /// verify that it won't be copied, and we don't trust our users not to
    /// copy it. It is kept so that we can move all usages back at once into
    /// structs should this be a perf issue in the future.
    public class MultipleUseFlag
    {
        private int _state;

        public MultipleUseFlag(MultipleUseFlag other)
        {
            throw new InvalidOperationException($"Copy of {nameof(MultipleUseFlag)} is forbidden");
        }

        /// <summary>
        /// Creates a flag.
        /// </summary>
        /// <param name="raised">if it should be raised or not</param>
        public MultipleUseFlag(bool raised = false)
        {
            _state = 0;
            if (raised)
                Interlocked.Exchange(ref _state, 1);
        }

        /// <summary>
        /// Raises the flag. If already up, throws InvalidOperationException.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RaiseOrDie()
        {
            if (Raise() == false)
                ThrowRaiseException();
        }

        /// <summary>
        /// This is here to allow RaiseOrDie() to be inlined.
        /// </summary>
        private static void ThrowRaiseException()
        {
            throw new InvalidOperationException($"Repeated Raise for a {nameof(MultipleUseFlag)} instance");
        }

        /// <summary>
        /// Lowers the flag. If already low, throws InvalidOperationException.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LowerOrDie()
        {
            if (Lower() == false)
                ThrowLowerException();
        }

        /// <summary>
        /// This is here to allow LowerOrDie() to be inlined.
        /// </summary>
        private static void ThrowLowerException()
        {
            throw new InvalidOperationException($"Repeated Lower for a {nameof(MultipleUseFlag)} instance");
        }

        /// <summary>
        /// Lowers the flag
        /// </summary>
        /// <returns>If already low, false; otherwise, true</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Lower()
        {
            return Interlocked.CompareExchange(ref _state, 0, 1) == 1;
        }

        /// <summary>
        /// Raises the flag
        /// </summary>
        /// <returns>If already raised, false; otherwise, true</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Raise()
        {
            return Interlocked.CompareExchange(ref _state, 1, 0) == 0;
        }

        /// <returns>True iff the flag is raised</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsRaised()
        {
            return _state != 0;
        }

        /// <summary>
        /// Returns true iff the flag is raised. Same as calling IsRaised().
        /// </summary>
        /// <param name="flag">Flag to check</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator bool(MultipleUseFlag flag)
        {
            return flag.IsRaised();
        }
    }
}
