using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sparrow.Json
{
    public partial class JsonOperationContext
    {
        [ThreadStatic]
        private static DisposalGuard _activeDisposalGuard;

        /// <summary>
        /// DEBUG only: until the scope is disposed, disposing a blittable or a lazy string that is not owned by <paramref name="context"/>
        /// (or by a context allocated during the scope) throws, since such an object outlives the current execution
        /// </summary>
        public static DisposalGuardScope RestrictDisposalTo(JsonOperationContext context, string description)
        {
#if DEBUG
            return new DisposalGuardScope(new DisposalGuard(context, description));
#else
            return default;
#endif
        }

        [Conditional("DEBUG")]
        internal static void AssertDisposalAllowed(JsonOperationContext owner, object disposedObject)
        {
            var guard = _activeDisposalGuard;
            if (guard == null || owner == null || guard.IsAllowed(owner))
                return;

            ThrowDisposalNotAllowed(guard, owner, disposedObject);
        }

        /// <summary>
        /// DEBUG only: the objects owned by <paramref name="context"/> may be disposed during the current guarded execution
        /// </summary>
        [Conditional("DEBUG")]
        public static void AllowDisposalFrom(JsonOperationContext context)
        {
            _activeDisposalGuard?.Allow(context);
        }

        [Conditional("DEBUG")]
        internal static void RegisterContextAllocation(JsonOperationContext context)
        {
            _activeDisposalGuard?.Allow(context);
        }

        private static void ThrowDisposalNotAllowed(DisposalGuard guard, JsonOperationContext owner, object disposedObject)
        {
            throw new InvalidOperationException(
                $"Attempt to dispose a '{disposedObject.GetType().Name}' (owned by context '{owner}') while the disposal of context-owned objects " +
                $"is restricted to the ones owned by context '{guard.Context}' ({guard.Description}). " +
                "An object that outlives the current execution must not be disposed from it, since the same execution may run again with a different context " +
                "and would then operate on a disposed object. Dispose it where it is owned (e.g. when the command is disposed) instead.");
        }

        internal sealed class DisposalGuard
        {
            public readonly JsonOperationContext Context;
            public readonly string Description;

            private HashSet<JsonOperationContext> _allocatedInScope;

            public DisposalGuard(JsonOperationContext context, string description)
            {
                Context = context;
                Description = description;
            }

            public void Allow(JsonOperationContext context)
            {
                _allocatedInScope ??= new HashSet<JsonOperationContext>();
                _allocatedInScope.Add(context);
            }

            public bool IsAllowed(JsonOperationContext context)
            {
                return ReferenceEquals(context, Context) || _allocatedInScope?.Contains(context) == true;
            }
        }

        public readonly struct DisposalGuardScope : IDisposable
        {
            private readonly DisposalGuard _previous;

            internal DisposalGuardScope(DisposalGuard guard)
            {
                _previous = _activeDisposalGuard;
                _activeDisposalGuard = guard;
            }

            public void Dispose()
            {
#if DEBUG
                _activeDisposalGuard = _previous;
#endif
            }
        }
    }
}
