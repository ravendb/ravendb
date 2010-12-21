//-----------------------------------------------------------------------
// <copyright file="IClientSideDatabase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Client.Indexes
{
    /// <summary>
    /// DatabaseAccessor for loading documents in the translator
    /// </summary>
    public interface IClientSideDatabase
    {
        /// <summary>
        /// Loading a document during result transformers
        /// </summary>
        T Load<T>(string docId);
    }
}