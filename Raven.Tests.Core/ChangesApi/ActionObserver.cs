// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Tests.Core.ChangesApi
{
    public class ActionObserver<T> : IObserver<T>
    {
        private readonly Action<T> action;

        public ActionObserver(Action<T> action)
        {
            this.action = action;
        }

        public void OnNext(T notification)
        {
            action(notification);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}