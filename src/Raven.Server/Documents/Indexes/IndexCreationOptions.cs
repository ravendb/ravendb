//-----------------------------------------------------------------------
// <copyright file="IndexCreationOptions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Server.Documents.Indexes
{
    public enum IndexCreationOptions
    {
        Noop,
        Update,
        Create,
        UpdateWithoutUpdatingCompiledIndex
    }
}
