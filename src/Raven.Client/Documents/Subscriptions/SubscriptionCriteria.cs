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
        private class SubscriptionCriteriaConvertor : JavascriptConversionExtension
        {
            public ParameterExpression Parameter;

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var node = context.Node as MemberExpression;
                if (node == null )
                    return;

                context.PreventDefault();
                var javascriptWriter = context.GetWriter();

                using (javascriptWriter.Operation(node))
                {
                    if (node.Expression == Parameter)
                    {
                        javascriptWriter.Write("this.");
                        javascriptWriter.Write(node.Member.Name);
                        return;
                    }

                    context.Visitor.Visit(node.Expression as MemberExpression);
                    javascriptWriter.Write(".");
                    javascriptWriter.Write(node.Member.Name);
                }
            }
        }


        public SubscriptionCriteria(Expression<Func<T, bool>> predicate)
        {
            var script = predicate.CompileToJavascript(
                new JavascriptCompilationOptions(
                    JsCompilationFlags.BodyOnly, 
                    new SubscriptionCriteriaConvertor { Parameter = predicate.Parameters[0] }));
            Script = $"return {script};";
        }
        
        public SubscriptionCriteria()
        {
            
        }
        
        public string Script { get; set; }
        public bool? IsVersioned { get; set; }
    }
    
    
    public class SubscriptionTryout
    {
        public ChangeVectorEntry[] ChangeVector { get; set; }
        public string Collection { get; set; }
        public string Script { get; set; }
        public bool IsVersioned { get; set; }
    }

    public class SubscriptionCreationOptions
    {
        public const string DefaultVersioningScript = "return {Current:this.Current, Previous:this.Previous};";
        public string Name { get; set; }
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

        public string Name { get; set; }
        public SubscriptionCriteria<T> Criteria { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
    }

    public class Versioned<T> where T : class
    {
        public T Previous;
        public T Current;
    }
}
