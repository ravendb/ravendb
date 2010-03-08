using System.Collections.Generic;
using System.IO;
using ICSharpCode.NRefactory;
using ICSharpCode.NRefactory.Ast;
using Lucene.Net.Search;

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

        public DynamicViewCompiler(string name, string map, string reduce)
        {
            this.name = name;
            this.map = map;
            this.reduce = reduce;
        }

        //private void TransformQueryToClass()
        //{
        //    var variableDeclaration = QueryParsingUtils.GetVariableDeclaration(map);
        //    var queryExpression = ((QueryExpression)variableDeclaration.Initializer);
        //    var selectOrGroupClause = queryExpression.SelectOrGroupClause;
        //    var projection = ((QueryExpressionSelectClause)selectOrGroupClause).Projection;
        //    var objectInitializer = ((ObjectCreateExpression)projection).ObjectInitializer;

        //    var identifierExpression = new IdentifierExpression(queryExpression.FromClause.Identifier);
        //    objectInitializer.CreateExpressions.Add(
        //        new NamedArgumentExpression
        //        {
        //            Name = "__document_id",
        //            Expression = new MemberReferenceExpression(identifierExpression, "__document_id")
        //        });

        //    var type = new TypeDeclaration(Modifiers.Public, new List<AttributeSection>())
        //    {
        //        BaseTypes =
        //            {
        //                new TypeReference("AbstractIndexGenerator")
        //            },
        //        Name = Name,
        //        Type = ClassType.Class
        //    };

        //    var ctor = new ConstructorDeclaration(Name,
        //                                          Modifiers.Public,
        //                                          new List<ParameterDeclarationExpression>(), null);
        //    type.Children.Add(ctor);
        //    ctor.Body = new BlockStatement();

        //    // this.IndexText = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185"; // Will be replaced later
        //    ctor.Body.AddChild(new ExpressionStatement(
        //                           new AssignmentExpression(
        //                               new MemberReferenceExpression(new ThisReferenceExpression(), "IndexText"),
        //                               AssignmentOperatorType.Assign,
        //                               new PrimitiveExpression(indexTextToken, indexTextToken))));

        //    // this.CompiledDefinition = from doc in docs ...;
        //    ctor.Body.AddChild(new ExpressionStatement(
        //                           new AssignmentExpression(
        //                               new MemberReferenceExpression(new ThisReferenceExpression(), "CompiledDefinition"),
        //                               AssignmentOperatorType.Assign,
        //                               new LambdaExpression
        //                               {
        //                                   Parameters =
        //                                   {
        //                                       new ParameterDeclarationExpression(null, "docs")
        //                                   },
        //                                   ExpressionBody = variableDeclaration.Initializer
        //                               })));

        //    CompiledQueryText = GenerateText(type);
        //    CompiledQueryText = CompiledQueryText.Replace("\"" + indexTextToken + "\"",
        //                                                  "@\"" + Query.Replace("\"", "\"\"") + "\"");
        //}
    }
}