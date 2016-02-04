// -----------------------------------------------------------------------
//  <copyright file="SmugglerNotifications.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Smuggler.Common
{
    public abstract class SmugglerNotifications
    {
        public EventHandler<string> OnProgress = (sender, message) => { };

        public void ShowProgress(string format, params object[] args)
        {
            try
            {
                var message = string.Format(format, args);
                Console.WriteLine(message);
                OnProgress(this, message);
            }
            catch (FormatException e)
            {
                throw new FormatException("Input string is invalid: " + format + Environment.NewLine + string.Join(", ", args), e);
            }
        }
    }
}