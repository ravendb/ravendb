//-----------------------------------------------------------------------
// <copyright file="DisposableAsyncAction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading.Tasks;

namespace Tests.Infrastructure.Utils
{
    /// <summary>
    /// A helper class that translate between Disposable and async action
    /// </summary>
    public class DisposableAsyncAction : IAsyncDisposable
    {
        private readonly Func<Task> _action;

        /// <summary>
        /// Initializes a new instance of the <see cref="DisposableAsyncAction"/> class.
        /// </summary>
        /// <param name="action">The async action.</param>
        public DisposableAsyncAction(Func<Task> action)
        {
            _action = action;
        }

        /// <summary>
        /// Execute the relevant action
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            await _action().ConfigureAwait(false);
        }
    }
}
