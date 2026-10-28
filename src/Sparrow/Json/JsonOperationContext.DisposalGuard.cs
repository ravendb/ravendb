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
        /// Until the returned scope is disposed, disposing (on the current thread) a context-owned object (<see cref="BlittableJsonReaderObject"/>,
        /// <see cref="LazyStringValue"/>) that is owned by a context other than <paramref name="context"/>, or than a context that was allocated
        /// (on the current thread) while the scope is active, is reported as an error.
        /// <para>
        /// This is meant for code that may be executed more than once with a different context each time (e.g. a transaction merger command),
        /// where the objects that are allocated from the current context (or from a context that lives only during the current execution)
        /// die with it anyway, while disposing an object that outlives the current execution (one that is owned by the caller) would break
        /// the next execution.
        /// </para>
        /// <para>The verification is done in DEBUG builds only.</para>
        /// </summary>
        public static DisposalGuardScope RestrictDisposalTo(JsonOperationContext context, string description)
        {
            return new DisposalGuardScope(new DisposalGuard(context, description));
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
        /// Declares that the objects owned by <paramref name="context"/> may be disposed during the current guarded execution (on the current thread),
        /// for a context that the execution deliberately allocates from and consumes (e.g. to let some output outlive the execution).
        /// Has no effect when no disposal guard is active.
        /// </summary>
        [Conditional("DEBUG")]
        public static void AllowDisposalFrom(JsonOperationContext context)
        {
            _activeDisposalGuard?.Allow(context);
        }

        /// <summary>
        /// A context that is allocated while a disposal guard is active (on the current thread) lives only during the guarded execution,
        /// so the objects that it owns may be disposed during it
        /// </summary>
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
                _activeDisposalGuard = _previous;
            }
        }
    }
}
