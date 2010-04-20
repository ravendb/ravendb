using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Web;
using ICSharpCode.NRefactory.Ast;
using Raven.Database.Indexing;

namespace Raven.Database.Linq
{
	/// <summary>
	/// 	Takes two query expressions as strings, and compile them.
	/// 	Along the way we apply some minimal transofrmations, the end result is an instance
	/// 	of AbstractViewGenerator, representing the map/reduce fucntions
	/// </summary>
	public class DynamicViewCompiler
	{
		private readonly IndexDefinition indexDefinition;
		private const string mapReduceTextToken = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185";

		private readonly string name;

		public DynamicViewCompiler(string name, IndexDefinition indexDefinition)
		{
			this.indexDefinition = indexDefinition;
			this.name = HttpUtility.UrlEncode(name);
		}

		public string CompiledQueryText { get; set; }
		public Type GeneratedType { get; set; }

		public string Name
		{
			get { return name; }
		}

		public string CSharpSafeName { get; set; }

		private void TransformQueryToClass()
		{
			var mapDefinition = TransformMapDefinition();

			CSharpSafeName = Regex.Replace(Name, @"[^\w\d]", "_");
			var type = new TypeDeclaration(Modifiers.Public, new List<AttributeSection>())
			{
				BaseTypes =
					{
						new TypeReference("AbstractViewGenerator")
					},
				Name = CSharpSafeName,
				Type = ClassType.Class
			};

			var ctor = new ConstructorDeclaration(CSharpSafeName,
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


			if (indexDefinition.IsMapReduce)
			{
				var reduceDefiniton = QueryParsingUtils.GetVariableDeclaration(indexDefinition.Reduce);
				// this.ReduceDefinition = from result in results...;
				ctor.Body.AddChild(new ExpressionStatement(
				                   	new AssignmentExpression(
				                   		new MemberReferenceExpression(new ThisReferenceExpression(),
				                   		                              "ReduceDefinition"),
				                   		AssignmentOperatorType.Assign,
				                   		new LambdaExpression
				                   		{
				                   			Parameters =
				                   				{
				                   					new ParameterDeclarationExpression(null, "results")
				                   				},
				                   			ExpressionBody = reduceDefiniton.Initializer
				                   		})));
				var sourceSelect = (QueryExpression) ((QueryExpression) reduceDefiniton.Initializer).FromClause.InExpression;
				var groupBySource = ((QueryExpressionGroupClause) sourceSelect.SelectOrGroupClause).GroupBy;
				var groupByField = ((MemberReferenceExpression) groupBySource).MemberName;
				ctor.Body.AddChild(new ExpressionStatement(
				                   	new AssignmentExpression(
				                   		new MemberReferenceExpression(new ThisReferenceExpression(),
				                   		                              "GroupByField"),
				                   		AssignmentOperatorType.Assign,
				                   		new PrimitiveExpression(groupByField, groupByField))));
			}

			CompiledQueryText = QueryParsingUtils.GenerateText(type);
			var compiledQueryText = "@\"" + indexDefinition.Map.Replace("\"", "\"\"");
			if (indexDefinition.Reduce != null)
			{
				compiledQueryText += Environment.NewLine + indexDefinition.Reduce.Replace("\"", "\"\"");
			}

			compiledQueryText += "\"";
			CompiledQueryText = CompiledQueryText.Replace("\"" + mapReduceTextToken + "\"",
			                                              compiledQueryText);
		}

		private VariableDeclaration TransformMapDefinition()
		{
			var variableDeclaration = QueryParsingUtils.GetVariableDeclaration(indexDefinition.Map);
			var queryExpression = ((QueryExpression) variableDeclaration.Initializer);
			var expression = queryExpression.FromClause.InExpression;
			if(expression is MemberReferenceExpression) // collection
			{
				var mre = (MemberReferenceExpression)expression;
				queryExpression.FromClause.InExpression = mre.TargetObject;
				//doc["@metadata"]["Raven-Entity-Name"]
				var metadata = new IndexerExpression(
					new IndexerExpression(new IdentifierExpression(queryExpression.FromClause.Identifier), new List<Expression> { new PrimitiveExpression("@metadata", "@metadata") }),
					new List<Expression> { new PrimitiveExpression("Raven-Entity-Name", "Raven-Entity-Name") }
					);
				queryExpression.MiddleClauses.Insert(0, 
					new QueryExpressionWhereClause
					{
						Condition = 
							new BinaryOperatorExpression(
								metadata,
								BinaryOperatorType.Equality,
								new PrimitiveExpression(mre.MemberName, mre.MemberName)
								)
					});
			}
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
			return variableDeclaration;
		}

		public AbstractViewGenerator GenerateInstance()
		{
			TransformQueryToClass();
			GeneratedType = QueryParsingUtils.Compile(CSharpSafeName, CompiledQueryText);
			return (AbstractViewGenerator) Activator.CreateInstance(GeneratedType);
		}
	}
}