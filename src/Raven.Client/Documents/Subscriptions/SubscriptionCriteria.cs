using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
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
        public bool Disabled { get; set; }
    }

    public class SubscriptionCreationOptions<T>
    {
        public string Name { get; set; }
        public bool Disabled { get; set; }
        public Expression<Func<T, bool>> Filter { get; set; }
        public Expression<Func<T, object>> Projection { get; set; }
        public Action<ISubscriptionIncludeBuilder<T>> Includes { get; set; }
        public string ChangeVector { get; set; }
        public string MentorNode { get; set; }

        public SubscriptionCreationOptions ToSubscriptionCreationOptions(DocumentConventions conventions)
        {
            SubscriptionCreationOptions subscriptionCreationOptions = new SubscriptionCreationOptions
            {
                Name = Name,
                ChangeVector = ChangeVector,
                MentorNode = MentorNode,
                Disabled = Disabled
            };
            return DocumentSubscriptions.CreateSubscriptionOptionsFromGeneric(conventions, 
                subscriptionCreationOptions, Filter, Projection, Includes);
        }
    }


    public class Revision<T> where T : class
    {
        public T Previous;
        public T Current;
    }
}
