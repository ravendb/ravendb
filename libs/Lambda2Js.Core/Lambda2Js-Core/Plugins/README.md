# Plugins

Lambda2Js supports plugins that allows anyone to customize the conversion process.

You tell the compiler that you want to use plugins when calling `CompileToJavascript` method.

Samples
-------

Custom JavaScript output when calling a method in a custom class:

    public class MyCustomClassMethods : JavascriptConversionExtension
    {
        public override void ConvertToJavascript(JavascriptConversionContext context)
        {
            var methodCall = context.Node as MethodCallExpression;
            if (methodCall != null)
                if (methodCall.Method.DeclaringType == typeof(MyCustomClass))
                {
                    switch (methodCall.Method.Name)
                    {
                        case "GetValue":
                        {
                            using (context.Operation(JavascriptOperationTypes.Call))
                            {
                                using (context.Operation(JavascriptOperationTypes.IndexerProperty))
                                    context.Write("Xpto.GetValue");

                                context.WriteManyIsolated('(', ')', ',', methodCall.Arguments);
                            }

                            return;
                        }
                    }
                }
        }
    }
