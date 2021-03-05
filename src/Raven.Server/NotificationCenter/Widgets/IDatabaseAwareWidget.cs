// -----------------------------------------------------------------------
//  <copyright file="IDatabaseAwareWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Server.NotificationCenter.Widgets
{
    public interface IDatabaseAwareWidget
    {
        public string Database
        {
            get;
        }
    }
}
