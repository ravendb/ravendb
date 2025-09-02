using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Session.Loaders;

namespace Raven.Client.Documents.Subscriptions
{
    internal sealed class SubscriptionTryout
    {
        public string ChangeVector { get; set; }
        public string Query { get; set; }
        public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }
    }

    internal interface ISubscriptionCreationOptions
    {
        public string Name { get; set; }
        public string ChangeVector { get; set; }
        public string MentorNode { get; set; }
        public  bool Disabled { get; set; }
        public  bool PinToMentorNode { get; set; }
        public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }
    }

    /// <summary>
    /// Options for creating a subscription using a typed predicate, projection and includes.
    /// </summary>
    public sealed class PredicateSubscriptionCreationOptions : ISubscriptionCreationOptions
    {
        public string Name { get; set; }
        public string ChangeVector { get; set; }
        public string MentorNode { get; set; }
        public bool Disabled { get; set; }
        public bool PinToMentorNode { get; set; }
        public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }

        internal SubscriptionCreationOptions ToSubscriptionCreationOptions()
        {
            return new SubscriptionCreationOptions
            {
                Name = Name,
                ChangeVector = ChangeVector,
                MentorNode = MentorNode,
                PinToMentorNode = PinToMentorNode,
                Disabled = Disabled,
                ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior
            };
        }
    }

    /// <summary>
    /// Options for creating a subscription using raw RQL and connection parameters.
    /// </summary>
    public class SubscriptionCreationOptions : ISubscriptionCreationOptions
    {
        public string Name { get; set; }
        public string Query { get; set; }
        public string ChangeVector { get; set; }
        public string MentorNode { get; set; }
        public virtual bool Disabled { get; set; }
        public virtual bool PinToMentorNode { get; set; }
        public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }
    }

    /// <summary>
    /// Strongly-typed helper for creating subscription options with predicate, projection and includes.
    /// </summary>
    public sealed class SubscriptionCreationOptions<T>
    {
        public string Name { get; set; }
        public bool Disabled { get; set; }
        public Expression<Func<T, bool>> Filter { get; set; }
        public Expression<Func<T, object>> Projection { get; set; }
        public Action<ISubscriptionIncludeBuilder<T>> Includes { get; set; }
        public string ChangeVector { get; set; }
        public string MentorNode { get; set; }
        public bool PinToMentorNode { get; set; }

        public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }

        /// <summary>
        /// Builds concrete <see cref="SubscriptionCreationOptions"/> from the typed options using the provided conventions.
        /// </summary>
        /// <param name="conventions">The document conventions to use when translating the typed options into an RQL subscription.</param>
        /// <returns>A populated <see cref="SubscriptionCreationOptions"/>.</returns>
        public SubscriptionCreationOptions ToSubscriptionCreationOptions(DocumentConventions conventions)
        {
            SubscriptionCreationOptions subscriptionCreationOptions = new SubscriptionCreationOptions
            {
                Name = Name,
                ChangeVector = ChangeVector,
                MentorNode = MentorNode,
                PinToMentorNode = PinToMentorNode,
                Disabled = Disabled,
                ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior
            };
            return DocumentSubscriptions.CreateSubscriptionOptionsFromGeneric(conventions, 
                subscriptionCreationOptions, Filter, Projection, Includes);
        }
    }

    /// <summary>
    /// Options for updating an existing subscription, including toggles that track whether values were set.
    /// </summary>
    public class SubscriptionUpdateOptions : SubscriptionCreationOptions
    {
        public long? Id { get; set; }
        public bool CreateNew { get; set; }

        private bool _pinToMentorNode;

        public override bool PinToMentorNode
        {
            get => _pinToMentorNode;
            set
            {
                _pinToMentorNode = value;
                PinToMentorNodeWasSet = true;
            }
        }

        internal bool PinToMentorNodeWasSet { get; set; }

        private bool _disabled;

        public override bool Disabled
        {
            get => _disabled;
            set
            {
                _disabled = value;
                DisabledWasSet = true;
            }
        }

        internal bool DisabledWasSet { get; set; }
    }

    /// <summary>
    /// Represents a pair of document states: the previous and the current version, used when including revisions in subscriptions.
    /// </summary>
    public sealed class Revision<T> where T : class
    {
        public T Previous;
        public T Current;
    }
}
