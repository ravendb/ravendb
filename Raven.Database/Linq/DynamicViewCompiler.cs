using System;
using System.Collections.Generic;
using ICSharpCode.NRefactory.Ast;

namespace Raven.Database.Linq
{
    /// <summary>
    ///   Takes two query expressions as strings, and compile them.
    ///   Along the way we apply some minimal transofrmations, the end result is an instance
    ///   of AbstractViewGenerator, representing the map/reduce fucntions
    /// </summary>
    public class DynamicViewCompiler 
    {
        private const string mapReduceTextToken = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185";
        
        private readonly string name;
        private readonly string map;
        private readonly string reduce;
        public string CompiledQueryText { get; set; }
        public Type GeneratedType { get; set; }

        public DynamicViewCompiler(string name, string map, string reduce)
        {
            this.name = name;
            this.map = map;
            this.reduce = reduce;
        }

        public string Name
        {
            get { return name; }
        }

        private void TransformQueryToClass()
        {
            var mapDefinition = TransformMapDefinition();

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
                                       new PrimitiveExpression(mapReduceTextToken, mapReduceTextToken))));

            // this.MapDefinition = from doc in docs ...;
            ctor.Body.AddChild(new ExpressionStatement(
                                   new AssignmentExpression(
                                       new MemberReferenceExpression(new ThisReferenceExpression(), "MapDefinition"),
                                       AssignmentOperatorType.Assign,
                                       new LambdaExpression
                                       {
                                           Parameters =
                                           {
                                               new ParameterDeclarationExpression(null, "docs")
                                           },
                                           ExpressionBody = mapDefinition.Initializer
                                       })));


            var reduceDefiniton = QueryParsingUtils.GetVariableDeclaration(reduce);
            // this.ReduceDefinition = from result in results...;
            ctor.Body.AddChild(new ExpressionStatement(
                                   new AssignmentExpression(
                                       new MemberReferenceExpression(new ThisReferenceExpression(), "ReduceDefinition "),
                                       AssignmentOperatorType.Assign,
                                       new LambdaExpression
                                       {
                                           Parameters =
                                           {
                                               new ParameterDeclarationExpression(null, "results")
                                           },
                                           ExpressionBody = reduceDefiniton.Initializer
                                       })));

            CompiledQueryText = QueryParsingUtils.GenerateText(type);
            CompiledQueryText = CompiledQueryText.Replace("\"" + mapReduceTextToken + "\"",
                                                          "@\"" + map.Replace("\"", "\"\"") + Environment.NewLine + reduce.Replace("\"", "\"\"") + "\"");
        }

        private VariableDeclaration TransformMapDefinition()
        {
            var variableDeclaration = QueryParsingUtils.GetVariableDeclaration(map);
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
            return variableDeclaration;
        }

        public AbstractViewGenerator GenerateInstance()
        {
            TransformQueryToClass();
            GeneratedType = QueryParsingUtils.Compile(name, CompiledQueryText);
            return (AbstractViewGenerator)Activator.CreateInstance(GeneratedType);
        }
    }
}