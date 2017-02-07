//-----------------------------------------------------------------------
// <copyright file="CascadeDeleteContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Database.Data;

namespace Raven.Bundles.CascadeDelete
{
    /// <summary>
    /// CascadeDeleteContext is required (and we are not using Database.DisableAllTriggersForCurrentThread()) for two reasons:
    /// a) We are modifying the database while in operation, and we don't want to change the behavior of other bundles.
    ///	   For example, we do want to replicate a cascade delete resulting from this bundle.
    /// b) We are using the context to track state as well.
    /// </summary>
    public static class CascadeDeleteContext
    {
        private static readonly ThreadLocal<bool> CurrentlyInContext = new ThreadLocal<bool>();

        private static readonly ThreadLocal<HashSet<string>> DeletedDocuments =
            new ThreadLocal<HashSet<string>>(() => new HashSet<string>(StringComparer.InvariantCultureIgnoreCase));

        public static bool IsInCascadeDeleteContext
        {
            get
            {
                return CurrentlyInContext.Value;
            }
        }

        public static bool HasAlreadyDeletedDocument(string key)
        {
            return DeletedDocuments.Value.Contains(key);
        }

        public static void AddDeletedDocument(string key)
        {
            DeletedDocuments.Value.Add(key);
        }

        public static IDisposable Enter()
        {
            var oldCurrentlyInContext = CurrentlyInContext.Value;
            var oldDeletedDocuments = DeletedDocuments.Value;
            CurrentlyInContext.Value = true;
            DeletedDocuments.Value = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
            return new DisposableAction(delegate
            {
                CurrentlyInContext.Value = oldCurrentlyInContext;
                DeletedDocuments.Value = oldDeletedDocuments;
            });
        }
    }

}
