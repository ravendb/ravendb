using System;
using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using Lambda2Js;
using Raven.Client.Documents.Conventions;

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
        public bool IncludeRevisions { get; set; }
    }

    public class SubscriptionCriteria<T>
    {
        private class LinqMethodsSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var methodCallExpression = context.Node as MethodCallExpression;
                var methodName = methodCallExpression?
                    .Method.Name;

                if (methodName == null)
                    return;

                string newName;
                switch (methodName)
                {
                    case "Any":
                        newName = "some";
                        break;
                    case "All":
                        newName = "every";
                        break;
                    case "Select":
                        newName = "map";
                        break;
                    case "Where":
                        newName = "filter";
                        break;
                    case "Contains":
                        newName = "indexOf";
                        break;
                    default:
                        return;

                }
                var javascriptWriter = context.GetWriter();

                var obj = methodCallExpression.Arguments[0] as MemberExpression;
                if (obj == null)
                {
                    if (methodCallExpression.Arguments[0] is MethodCallExpression innerMethodCall)
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(innerMethodCall);
                        javascriptWriter.Write($".{newName}");
                    }
                    else return;
                }
                else
                {
                    context.PreventDefault();
                    javascriptWriter.Write($"this.{obj.Member.Name}.{newName}");
                }

                if (methodCallExpression.Arguments.Count < 2)
                    return;

                javascriptWriter.Write("(");
                context.Visitor.Visit(methodCallExpression.Arguments[1]);
                javascriptWriter.Write(")");

                if (newName == "indexOf")
                {
                    javascriptWriter.Write(">=0");
                }
            }
        }


        private class SubscriptionCriteriaConvertor : JavascriptConversionExtension
        {
            public ParameterExpression Parameter;

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var nodeAsConst = context.Node as ConstantExpression;

                if (nodeAsConst != null && nodeAsConst.Type == typeof(bool))
                {
                    context.PreventDefault();
                    var writer = context.GetWriter();
                    var val = nodeAsConst.Value.ToString().ToLower();

                    using (writer.Operation(nodeAsConst))
                    {
                        writer.Write(val);
                    }

                    return;
                }                
                
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
                    if (node.Expression is MemberExpression member)
                    {
                        context.Visitor.Visit(member);
                    }
                    else
                    {
                        context.Visitor.Visit(node.Expression);
                    }

                    javascriptWriter.Write(".");

                    if (node.Member.Name == "Count" && IsCollection(node.Member.DeclaringType))
                    {
                        javascriptWriter.Write("length");
                    }
                    else
                    {
                        javascriptWriter.Write(node.Member.Name);
                    }
                }
            }

            private static bool IsCollection(Type type)
            {
                if (type.GetGenericArguments().Length == 0)
                    return false;

                return typeof(IEnumerable).IsAssignableFrom(type.GetGenericTypeDefinition());
            }
        }


        public SubscriptionCriteria(Expression<Func<T, bool>> predicate)
        {
            var script = predicate.CompileToJavascript(
                new JavascriptCompilationOptions(
                    JsCompilationFlags.BodyOnly,
                    new LinqMethodsSupport(),
                    new SubscriptionCriteriaConvertor { Parameter = predicate.Parameters[0] }
                    ));
            Script = $"return {script};";
        }
        
        public SubscriptionCriteria()
        {
            
        }
        
        public string Script { get; set; }
        public bool? IncludeRevisions { get; set; }
    }
    
    
    public class SubscriptionTryout
    {
        public string ChangeVector { get; set; }
        public string Collection { get; set; }
        public string Script { get; set; }
        public bool IncludeRevisions { get; set; }
    }

    public class SubscriptionCreationOptions
    {
        public const string DefaultRevisionsScript = "return {Current:this.Current, Previous:this.Previous};";
        public string Name { get; set; }
        public SubscriptionCriteria Criteria { get; set; }
        public string ChangeVector { get; set; }
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
            var includeRevisions = tType.IsConstructedGenericType && tType.GetGenericTypeDefinition() == typeof(Revision<>);

            return new SubscriptionCriteria(conventions.GetCollectionName(includeRevisions ? tType.GenericTypeArguments[0] : typeof(T)))
            {
                Script = Criteria?.Script ?? (includeRevisions ? SubscriptionCreationOptions.DefaultRevisionsScript : null),
                IncludeRevisions =  includeRevisions || (Criteria?.IncludeRevisions ?? false) 
            };

        }

        public string Name { get; set; }
        public SubscriptionCriteria<T> Criteria { get; set; }
        public string ChangeVector { get; set; }
    }

    public class Revision<T> where T : class
    {
        public T Previous;
        public T Current;
    }
}
