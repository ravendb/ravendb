using System;
using System.Linq.Expressions;
using Lambda2Js;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Replication.Messages;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionCriteria 
    {
        public SubscriptionCriteria()
        {
            // for deserialization
        }

        public SubscriptionCriteria(string collection)
        {
            Collection = collection ?? throw new ArgumentNullException(nameof(collection));
        }

        public string Collection { get;  set; }
        public string Script { get; set; }
        public bool IsVersioned { get; set; }
    }

    public class SubscriptionCriteria<T>
    {
        public SubscriptionCriteria(Expression<Func<T, bool>> predicate)
        {
            Script =
                "var " + predicate.Parameters[0].Name + " = this;" +
                Environment.NewLine +
                "return " + predicate.CompileToJavascript(
                 new JavascriptCompilationOptions(JsCompilationFlags.BodyOnly)) + ";";
        }
        
        public SubscriptionCriteria()
        {
            
        }
        
        public string Script { get; set; }
        public bool? IsVersioned { get; set; }
    }

    public class SubscriptionCreationOptions
    {
        public const string DefaultVersioningScript = "return {Current:this.Current, Previous:this.Previous};";

        public SubscriptionCriteria Criteria { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
    }

    public class SubscriptionCreationOptions<T>
    {

        public SubscriptionCreationOptions()
        {
            Criteria = new SubscriptionCriteria<T>();
        }

        public SubscriptionCriteria CreateOptions(DocumentConventions conventions)
        {
            var tType = typeof(T);
            var isVersioned = tType.IsConstructedGenericType && tType.GetGenericTypeDefinition() == typeof(Versioned<>);

            return new SubscriptionCriteria(conventions.GetCollectionName(isVersioned ? tType.GenericTypeArguments[0] : typeof(T)))
            {
                Script = Criteria?.Script ?? (isVersioned ? SubscriptionCreationOptions.DefaultVersioningScript : null),
                IsVersioned =  isVersioned || (Criteria?.IsVersioned ?? false) 
            };

        }
        
        public SubscriptionCriteria<T> Criteria { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
    }

    public class Versioned<T> where T : class
    {
        public T Previous;
        public T Current;
    }
}
