using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using ICSharpCode.NRefactory.Ast;
using Microsoft.CSharp;
using Microsoft.CSharp.RuntimeBinder;

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

            CompiledQueryText = QueryParsingUtils.GenerateText(type);
            CompiledQueryText = CompiledQueryText.Replace("\"" + indexTextToken + "\"",
                                                          "@\"" + Query.Replace("\"", "\"\"") + "\"");
        }

       

        public AbstractIndexGenerator CreateInstance()
        {
            TransformQueryToClass();
            GeneratedType = QueryParsingUtils.Compile(Name, CompiledQueryText);
            GeneratedInstance = (AbstractIndexGenerator) Activator.CreateInstance(GeneratedType);
            return GeneratedInstance;
        }
    }
}