using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Session.Loaders;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionTryout
    {
        public string ChangeVector { get; set; }
        public string Query { get; set; }
    }

    public class SubscriptionCreationOptions
    {
        public string Name { get; set; }
        public string Query { get; set; }
        public string ChangeVector { get; set; }
        public string MentorNode { get; set; }
    }

    public class SubscriptionCreationOptions<T>
    {
        public string Name { get; set; }
        public Expression<Func<T, bool>> Filter { get; set; }
        public Expression<Func<T, object>> Projection { get; set; }
        public Action<ISubscriptionIncludeBuilder<T>> Includes { get; set; }
        public string ChangeVector { get; set; }
        public string MentorNode { get; set; }
    }


    public class Revision<T> where T : class
    {
        public T Previous;
        public T Current;
    }
}
