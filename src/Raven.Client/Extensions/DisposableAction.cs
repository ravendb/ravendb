//-----------------------------------------------------------------------
// <copyright file="DisposableAction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Extensions
{
    /// <summary>
    /// A helper class that translate between Disposable and Action
    /// </summary>
    public class DisposableAction : IDisposable
    {
        private readonly Action _action;

        /// <summary>
        /// Initializes a new instance of the <see cref="DisposableAction"/> class.
        /// </summary>
        /// <param name="action">The action.</param>
        public DisposableAction(Action action)
        {
            _action = action;
        }

        /// <summary>
        /// Execute the relevant actions
        /// </summary>
        public void Dispose()
        {
            _action();
        }
    }
}
