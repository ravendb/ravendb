//-----------------------------------------------------------------------
// <copyright file="ApiTestHook.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;

    /// <summary>
    /// A disposable class that can set and reset the Api implementation.
    /// </summary>
    internal class ApiTestHook : IDisposable
    {
        /// <summary>
        /// The saved api.
        /// </summary>
        private readonly IJetApi savedApi;

        /// <summary>
        /// Initializes a new instance of the ApiTestHook class.
        /// </summary>
        /// <param name="newApi">
        /// A new IJetApi to be used by the Api. The original
        /// value will be restored when this object is disposed.
        /// </param>
        public ApiTestHook(IJetApi newApi)
        {
            this.savedApi = Api.Impl;
            Api.Impl = newApi;
        }

        /// <summary>
        /// Destroy an ApiTestHook. This restores the saved Api.
        /// </summary>
        public void Dispose()
        {
            Api.Impl = this.savedApi;
            GC.SuppressFinalize(this);
        }
    }
}