using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using ICSharpCode.NRefactory;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.PrettyPrinter;
using Microsoft.CSharp;
using Microsoft.CSharp.RuntimeBinder;
using Raven.Database.Linq.PrivateExtensions;

namespace Raven.Database.Linq
{
    public class DynamicQueryCompiler
    {
        const string viewTextToken = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185";

        public Type GeneratedType { get; private set; }

        public string CompiledQueryText { get; private set; }

        public AbstractViewGenerator GeneratedInstance { get; private set; }

        public string Name { get; private set; }

        public string Query { get; private set; }

        public DynamicQueryCompiler(string name, string query)
        {
            Name = name;
            Query = query;
        }

        public void Compile()
        {
            var provider = new CSharpCodeProvider(new Dictionary<string, string> { { "CompilerVersion", "v4.0" } });
            var results = provider.CompileAssemblyFromSource(new CompilerParameters
            {
                GenerateExecutable = false,
                GenerateInMemory = false,
                IncludeDebugInformation = true,
                ReferencedAssemblies =
                {
                   typeof (AbstractViewGenerator).Assembly.Location,
                   typeof (NameValueCollection).Assembly.Location,
                   typeof (Enumerable).Assembly.Location,
                   typeof (Binder).Assembly.Location,
                },
            }, CompiledQueryText);

            if (results.Errors.HasErrors)
            {
                var sb = new StringBuilder()
                    .AppendLine("Source code:")
                    .AppendLine(CompiledQueryText)
                    .AppendLine();
                foreach (CompilerError error in results.Errors)
                {
                    sb.AppendLine(error.ToString());
                }
                throw new InvalidOperationException(sb.ToString());
            }
            GeneratedType = results.CompiledAssembly.GetType(Name);
        }

        public void TransformQueryToClass()
        {
            var parser = ParserFactory.CreateParser(SupportedLanguage.CSharp, new StringReader("var q = " + Query));
            var variableDeclaration = GetVariableDeclaration(parser.ParseBlock());
            var queryExpression = ((QueryExpression)variableDeclaration.Initializer);
            var selectOrGroupClause = queryExpression.SelectOrGroupClause;
            var projection = ((QueryExpressionSelectClause)selectOrGroupClause).Projection;
            var objectInitializer = ((ObjectCreateExpression)projection).ObjectInitializer;

            var identifierExpression = new IdentifierExpression(queryExpression.FromClause.Identifier);
            objectInitializer.CreateExpressions.Add(
                new NamedArgumentExpression
                {
                    Name = "__document_id",
                    Expression = new MemberReferenceExpression(identifierExpression, "__document_id")
                });

            var type = new TypeDeclaration(Modifiers.Public, new List<AttributeSection>())
            {
                BaseTypes =
                    {
                        new TypeReference("AbstractViewGenerator")
                    },
                Name = Name,
                Type = ClassType.Class
            };

            var ctor = new ConstructorDeclaration(Name,
                                                  Modifiers.Public,
                                                  new List<ParameterDeclarationExpression>(), null);
            type.Children.Add(ctor);
            ctor.Body = new BlockStatement();

            // this.ViewText = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185"; // Will be replaced later
            ctor.Body.AddChild(new ExpressionStatement(
                                   new AssignmentExpression(
                                       new MemberReferenceExpression(new ThisReferenceExpression(), "ViewText"),
                                       AssignmentOperatorType.Assign,
                                       new PrimitiveExpression(viewTextToken, viewTextToken))));

            // this.CompiledDefinition = from doc in docs ...;
            ctor.Body.AddChild(new ExpressionStatement(
                                   new AssignmentExpression(
                                       new MemberReferenceExpression(new ThisReferenceExpression(), "CompiledDefinition"),
                                       AssignmentOperatorType.Assign,
                                       new LambdaExpression
                                       {
                                           Parameters =
                                           {
                                               new ParameterDeclarationExpression(null, "docs")
                                           },
                                           ExpressionBody = variableDeclaration.Initializer
                                       })));

            CompiledQueryText = GenerateText(type);
            CompiledQueryText = CompiledQueryText.Replace("\"" + viewTextToken + "\"",
                                                          "@\"" + Query.Replace("\"", "\"\"") + "\"");

        }

        private static string GenerateText(TypeDeclaration type)
        {
            var unit = new CompilationUnit();
            unit.AddChild(new Using(typeof(AbstractViewGenerator).Namespace));
            unit.AddChild(new Using(typeof(Enumerable).Namespace));
            unit.AddChild(new Using(typeof(int).Namespace));
            unit.AddChild(new Using(typeof(LinqOnDynamic).Namespace));
            unit.AddChild(type);

            var output = new CSharpOutputVisitor();
            unit.AcceptVisitor(output, null);

            return output.Text;
        }

        private static VariableDeclaration GetVariableDeclaration(INode block)
        {
            if (block.Children.Count != 1)
                throw new InvalidOperationException("Only one statement is allowed");

            var declaration = block.Children[0] as LocalVariableDeclaration;
            if (declaration == null)
                throw new InvalidOperationException("Only local variable decleration are allowed");

            if (declaration.Variables.Count != 1)
                throw new InvalidOperationException("Only one variable declaration is allowed");

            var variable = declaration.Variables[0];

            if (variable.Initializer == null)
                throw new InvalidOperationException("Variable declaration must have an initializer");

            var queryExpression = (variable.Initializer as QueryExpression);
            if (queryExpression == null)
                throw new InvalidOperationException("Variable initializer must be a query expression");

            var selectClause = queryExpression.SelectOrGroupClause as QueryExpressionSelectClause;
            if(selectClause == null)
                throw new InvalidOperationException("Variable initializer must be a select query expression");

            var createExpression = selectClause.Projection as ObjectCreateExpression;
            if(createExpression == null || createExpression.IsAnonymousType == false)
                throw new InvalidOperationException(
                    "Variable initializer must be a select query expression returning an anonymous object");

            return variable;
        }

        public AbstractViewGenerator CreateInstance()
        {
            TransformQueryToClass();
            Compile();
            GeneratedInstance = (AbstractViewGenerator)Activator.CreateInstance(GeneratedType);
            return GeneratedInstance;
        }

    }
}