//-----------------------------------------------------------------------
// <copyright file="AnonymousUserAccessMode.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Database.Server
{
    public enum AnonymousUserAccessMode
    {
        [Obsolete("Will be removed in 4.0")]
        All,
        [Obsolete("Will be removed in 4.0")]
        Get,
        None,
        Admin
    }
}
