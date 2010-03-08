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
    /// <summary>
    ///   Takes a query expression as a string, and compile it
    ///   Along the way we apply some minimal transofrmations, the end result is an instance
    ///   of AbstractIndexGenerator, representing the indexing function
    /// </summary>
    public class DynamicIndexCompiler
    {
        private const string indexTextToken = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185";

        public DynamicIndexCompiler(string name, string query)
        {
            Name = name;
            Query = query;
        }

        public Type GeneratedType { get; private set; }

        public string CompiledQueryText { get; private set; }

        public AbstractIndexGenerator GeneratedInstance { get; private set; }

        public string Name { get; private set; }

        public string Query { get; private set; }

        private void Compile()
        {
            var provider = new CSharpCodeProvider(new Dictionary<string, string> {{"CompilerVersion", "v4.0"}});
            var results = provider.CompileAssemblyFromSource(new CompilerParameters
            {
                GenerateExecutable = false,
                GenerateInMemory = false,
                IncludeDebugInformation = true,
                ReferencedAssemblies =
                                                                 {
                                                                     typeof (AbstractIndexGenerator).Assembly
                                                                 .Location,
                                                                     typeof (NameValueCollection).Assembly.
                                                                 Location,
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

        private void TransformQueryToClass()
        {
            var variableDeclaration = QueryParsingUtils.GetVariableDeclaration(Query);
            var queryExpression = ((QueryExpression) variableDeclaration.Initializer);
            var selectOrGroupClause = queryExpression.SelectOrGroupClause;
            var projection = ((QueryExpressionSelectClause) selectOrGroupClause).Projection;
            var objectInitializer = ((ObjectCreateExpression) projection).ObjectInitializer;

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
                        new TypeReference("AbstractIndexGenerator")
                    },
                Name = Name,
                Type = ClassType.Class
            };

            var ctor = new ConstructorDeclaration(Name,
                                                  Modifiers.Public,
                                                  new List<ParameterDeclarationExpression>(), null);
            type.Children.Add(ctor);
            ctor.Body = new BlockStatement();

            // this.IndexText = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185"; // Will be replaced later
            ctor.Body.AddChild(new ExpressionStatement(
                                   new AssignmentExpression(
                                       new MemberReferenceExpression(new ThisReferenceExpression(), "IndexText"),
                                       AssignmentOperatorType.Assign,
                                       new PrimitiveExpression(indexTextToken, indexTextToken))));

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
            CompiledQueryText = CompiledQueryText.Replace("\"" + indexTextToken + "\"",
                                                          "@\"" + Query.Replace("\"", "\"\"") + "\"");
        }

        private static string GenerateText(TypeDeclaration type)
        {
            var unit = new CompilationUnit();
            unit.AddChild(new Using(typeof (AbstractIndexGenerator).Namespace));
            unit.AddChild(new Using(typeof (Enumerable).Namespace));
            unit.AddChild(new Using(typeof (int).Namespace));
            unit.AddChild(new Using(typeof (LinqOnDynamic).Namespace));
            unit.AddChild(type);

            var output = new CSharpOutputVisitor();
            unit.AcceptVisitor(output, null);

            return output.Text;
        }

        public AbstractIndexGenerator CreateInstance()
        {
            TransformQueryToClass();
            Compile();
            GeneratedInstance = (AbstractIndexGenerator) Activator.CreateInstance(GeneratedType);
            return GeneratedInstance;
        }
    }
}