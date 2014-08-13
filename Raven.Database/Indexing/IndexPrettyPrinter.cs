// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using ICSharpCode.NRefactory.CSharp;
using ICSharpCode.NRefactory.PatternMatching;

namespace Raven.Database.Indexing
{
	public static class IndexPrettyPrinter
	{
		public static string Format(string code)
		{
			var expr = new CSharpParser().ParseExpression(code);
			// Wrap expression in parenthesized expression, this is necessary because the transformations
			// can't replace the root node of the syntax tree
			expr = new ParenthesizedExpression(expr);
			// Apply transformations
			new IntroduceQueryExpressions().Run(expr);
			new CombineQueryExpressions().Run(expr);
			// Unwrap expression
			expr = ((ParenthesizedExpression)expr).Expression;
			var format = expr.GetText(FormattingOptionsFactory.CreateAllman());
			return format;
		}

		#region Decompiler Logic
		// Copyright (c) 2011 AlphaSierraPapa for the SharpDevelop Team
		// 
		// Permission is hereby granted, free of charge, to any person obtaining a copy of this
		// software and associated documentation files (the "Software"), to deal in the Software
		// without restriction, including without limitation the rights to use, copy, modify, merge,
		// publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
		// to whom the Software is furnished to do so, subject to the following conditions:
		// 
		// The above copyright notice and this permission notice shall be included in all copies or
		// substantial portions of the Software.
		// 
		// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
		// INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
		// PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
		// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
		// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
		// DEALINGS IN THE SOFTWARE.

		// Code taken from ILSpy 2.1; slightly adjusted for NRefactory 5.2.

		/// <summary>
		/// Decompiles query expressions.
		/// Based on C# 4.0 spec, §7.16.2 Query expression translation
		/// </summary>
		public class IntroduceQueryExpressions
		{
			public void Run(AstNode compilationUnit)
			{
				DecompileQueries(compilationUnit);
				// After all queries were decompiled, detect degenerate queries (queries not property terminated with 'select' or 'group')
				// and fix them, either by adding a degenerate select, or by combining them with another query.
				foreach (QueryExpression query in compilationUnit.Descendants.OfType<QueryExpression>())
				{
					if (query.Clauses.First().GetType() != typeof (QueryFromClause)) continue;

					QueryFromClause fromClause = (QueryFromClause) query.Clauses.First();
					if (IsDegenerateQuery(query))
					{
						// introduce select for degenerate query
						query.Clauses.Add(new QuerySelectClause {Expression = new IdentifierExpression(fromClause.Identifier)});
					}
					// See if the data source of this query is a degenerate query,
					// and combine the queries if possible.
					QueryExpression innerQuery = fromClause.Expression as QueryExpression;
					while (IsDegenerateQuery(innerQuery))
					{
						QueryFromClause innerFromClause = (QueryFromClause) innerQuery.Clauses.First();
						if (fromClause.Identifier != innerFromClause.Identifier)
							break;
						// Replace the fromClause with all clauses from the inner query
						fromClause.Remove();
						QueryClause insertionPos = null;
						foreach (var clause in innerQuery.Clauses)
						{
							query.Clauses.InsertAfter(insertionPos, insertionPos = Detach(clause));
						}
						fromClause = innerFromClause;
						innerQuery = fromClause.Expression as QueryExpression;
					}
				}
			}

			private T Detach<T>(T astNode)
				where T : AstNode
			{
				astNode.Remove();
				return astNode;
			}

			bool IsDegenerateQuery(QueryExpression query)
			{
				if (query == null)
					return false;
				var lastClause = query.Clauses.LastOrDefault();
				return !(lastClause is QuerySelectClause || lastClause is QueryGroupClause);
			}

			void DecompileQueries(AstNode node)
			{
				QueryExpression query = DecompileQuery(node as InvocationExpression);
				if (query != null)
					node.ReplaceWith(query);
				for (AstNode child = (query ?? node).FirstChild; child != null; child = child.NextSibling)
				{
					DecompileQueries(child);
				}
			}

			QueryExpression DecompileQuery(InvocationExpression invocation)
			{
				if (invocation == null)
					return null;
				MemberReferenceExpression mre = invocation.Target as MemberReferenceExpression;
				if (mre == null)
					return null;
				switch (mre.MemberName)
				{
					case "Select":
						{
							if (invocation.Arguments.Count != 1)
								return null;
							string parameterName;
							Expression body;
							if (MatchSimpleLambda(invocation.Arguments.Single(), out parameterName, out body))
							{
								QueryExpression query = new QueryExpression();
								query.Clauses.Add(new QueryFromClause { Identifier = parameterName, Expression = Detach(mre.Target) });
								query.Clauses.Add(new QuerySelectClause { Expression = Detach(body) });
								return query;
							}
							return null;
						}
					case "GroupBy":
						{
							if (invocation.Arguments.Count == 2)
							{
								string parameterName1, parameterName2;
								Expression keySelector, elementSelector;
								if (MatchSimpleLambda(invocation.Arguments.ElementAt(0), out parameterName1, out keySelector)
									&& MatchSimpleLambda(invocation.Arguments.ElementAt(1), out parameterName2, out elementSelector)
									&& parameterName1 == parameterName2)
								{
									QueryExpression query = new QueryExpression();
									query.Clauses.Add(new QueryFromClause { Identifier = parameterName1, Expression = Detach(mre.Target) });
									query.Clauses.Add(new QueryGroupClause { Projection = Detach(elementSelector), Key = Detach(keySelector) });
									return query;
								}
							}
							else if (invocation.Arguments.Count == 1)
							{
								string parameterName;
								Expression keySelector;
								if (MatchSimpleLambda(invocation.Arguments.Single(), out parameterName, out keySelector))
								{
									QueryExpression query = new QueryExpression();
									query.Clauses.Add(new QueryFromClause { Identifier = parameterName, Expression = Detach(mre.Target) });
									query.Clauses.Add(new QueryGroupClause { Projection = new IdentifierExpression(parameterName), Key = Detach(keySelector) });
									return query;
								}
							}
							return null;
						}
					case "SelectMany":
						{
							if (invocation.Arguments.Count != 2)
								return null;
							string parameterName;
							Expression collectionSelector;
							if (!MatchSimpleLambda(invocation.Arguments.ElementAt(0), out parameterName, out collectionSelector))
								return null;
							LambdaExpression lambda = invocation.Arguments.ElementAt(1) as LambdaExpression;
							if (lambda != null && lambda.Parameters.Count == 2 && lambda.Body is Expression)
							{
								ParameterDeclaration p1 = lambda.Parameters.ElementAt(0);
								ParameterDeclaration p2 = lambda.Parameters.ElementAt(1);
								if (p1.Name == parameterName)
								{
									QueryExpression query = new QueryExpression();
									query.Clauses.Add(new QueryFromClause { Identifier = p1.Name, Expression = Detach(mre.Target) });
									query.Clauses.Add(new QueryFromClause { Identifier = p2.Name, Expression = Detach(collectionSelector) });
									query.Clauses.Add(new QuerySelectClause { Expression = Detach(((Expression)lambda.Body)) });
									return query;
								}
							}
							return null;
						}
					case "Where":
						{
							if (invocation.Arguments.Count != 1)
								return null;
							string parameterName;
							Expression body;
							if (MatchSimpleLambda(invocation.Arguments.Single(), out parameterName, out body))
							{
								QueryExpression query = new QueryExpression();
								query.Clauses.Add(new QueryFromClause { Identifier = parameterName, Expression = Detach(mre.Target) });
								query.Clauses.Add(new QueryWhereClause { Condition = Detach(body) });
								return query;
							}
							return null;
						}
					case "OrderBy":
					case "OrderByDescending":
					case "ThenBy":
					case "ThenByDescending":
						{
							if (invocation.Arguments.Count != 1)
								return null;
							string parameterName;
							Expression orderExpression;
							if (MatchSimpleLambda(invocation.Arguments.Single(), out parameterName, out orderExpression))
							{
								if (ValidateThenByChain(invocation, parameterName))
								{
									QueryOrderClause orderClause = new QueryOrderClause();
									InvocationExpression tmp = invocation;
									while (mre.MemberName == "ThenBy" || mre.MemberName == "ThenByDescending")
									{
										// insert new ordering at beginning
										orderClause.Orderings.InsertAfter(
											null, new QueryOrdering
											{
												Expression = Detach(orderExpression),
												Direction = (mre.MemberName == "ThenBy" ? QueryOrderingDirection.None : QueryOrderingDirection.Descending)
											});

										tmp = (InvocationExpression)mre.Target;
										mre = (MemberReferenceExpression)tmp.Target;
										MatchSimpleLambda(tmp.Arguments.Single(), out parameterName, out orderExpression);
									}
									// insert new ordering at beginning
									orderClause.Orderings.InsertAfter(
										null, new QueryOrdering
										{
											Expression = Detach(orderExpression),
											Direction = (mre.MemberName == "OrderBy" ? QueryOrderingDirection.None : QueryOrderingDirection.Descending)
										});

									QueryExpression query = new QueryExpression();
									query.Clauses.Add(new QueryFromClause { Identifier = parameterName, Expression = Detach(mre.Target) });
									query.Clauses.Add(orderClause);
									return query;
								}
							}
							return null;
						}
					case "Join":
					case "GroupJoin":
						{
							if (invocation.Arguments.Count != 4)
								return null;
							Expression source1 = mre.Target;
							Expression source2 = invocation.Arguments.ElementAt(0);
							string elementName1, elementName2;
							Expression key1, key2;
							if (!MatchSimpleLambda(invocation.Arguments.ElementAt(1), out elementName1, out key1))
								return null;
							if (!MatchSimpleLambda(invocation.Arguments.ElementAt(2), out elementName2, out key2))
								return null;
							LambdaExpression lambda = invocation.Arguments.ElementAt(3) as LambdaExpression;
							if (lambda != null && lambda.Parameters.Count == 2 && lambda.Body is Expression)
							{
								ParameterDeclaration p1 = lambda.Parameters.ElementAt(0);
								ParameterDeclaration p2 = lambda.Parameters.ElementAt(1);
								if (p1.Name == elementName1 && (p2.Name == elementName2 || mre.MemberName == "GroupJoin"))
								{
									QueryExpression query = new QueryExpression();
									query.Clauses.Add(new QueryFromClause { Identifier = elementName1, Expression = Detach(source1) });
									QueryJoinClause joinClause = new QueryJoinClause();
									joinClause.JoinIdentifier = elementName2;    // join elementName2
									joinClause.InExpression = Detach(source2);  // in source2
									joinClause.OnExpression = Detach(key1);     // on key1
									joinClause.EqualsExpression = Detach(key2); // equals key2
									if (mre.MemberName == "GroupJoin")
									{
										joinClause.IntoIdentifier = p2.Name; // into p2.Name
									}
									query.Clauses.Add(joinClause);
									query.Clauses.Add(new QuerySelectClause { Expression = Detach(((Expression)lambda.Body)) });
									return query;
								}
							}
							return null;
						}
					default:
						return null;
				}
			}

			/// <summary>
			/// Ensure that all ThenBy's are correct, and that the list of ThenBy's is terminated by an 'OrderBy' invocation.
			/// </summary>
			bool ValidateThenByChain(InvocationExpression invocation, string expectedParameterName)
			{
				if (invocation == null || invocation.Arguments.Count != 1)
					return false;
				MemberReferenceExpression mre = invocation.Target as MemberReferenceExpression;
				if (mre == null)
					return false;
				string parameterName;
				Expression body;
				if (!MatchSimpleLambda(invocation.Arguments.Single(), out parameterName, out body))
					return false;
				if (parameterName != expectedParameterName)
					return false;

				if (mre.MemberName == "OrderBy" || mre.MemberName == "OrderByDescending")
					return true;
				else if (mre.MemberName == "ThenBy" || mre.MemberName == "ThenByDescending")
					return ValidateThenByChain(mre.Target as InvocationExpression, expectedParameterName);
				else
					return false;
			}

			/// <summary>Matches simple lambdas of the form "a => b"</summary>
			bool MatchSimpleLambda(Expression expr, out string parameterName, out Expression body)
			{
				LambdaExpression lambda = expr as LambdaExpression;
				if (lambda != null && lambda.Parameters.Count == 1 && lambda.Body is Expression)
				{
					ParameterDeclaration p = lambda.Parameters.Single();
					if (p.ParameterModifier == ParameterModifier.None)
					{
						parameterName = p.Name;
						body = (Expression)lambda.Body;
						return true;
					}
				}
				parameterName = null;
				body = null;
				return false;
			}
		}

		/// <summary>
		/// Combines query expressions and removes transparent identifiers.
		/// </summary>
		public class CombineQueryExpressions
		{
			public void Run(AstNode compilationUnit)
			{
				CombineQueries(compilationUnit);
			}

			static readonly InvocationExpression castPattern = new InvocationExpression
			{
				Target = new MemberReferenceExpression
				{
					Target = new AnyNode("inExpr"),
					MemberName = "Cast",
					TypeArguments = { new AnyNode("targetType") }
				}
			};

			void CombineQueries(AstNode node)
			{
				for (AstNode child = node.FirstChild; child != null; child = child.NextSibling)
				{
					CombineQueries(child);
				}
				QueryExpression query = node as QueryExpression;
				if (query != null)
				{
					if (query.Clauses.First().GetType() != typeof (QueryFromClause)) return;

					QueryFromClause fromClause = (QueryFromClause) query.Clauses.First();
					QueryExpression innerQuery = fromClause.Expression as QueryExpression;
					if (innerQuery != null)
					{
						if (TryRemoveTransparentIdentifier(query, fromClause, innerQuery))
						{
							RemoveTransparentIdentifierReferences(query);
						}
						else
						{
							QueryContinuationClause continuation = new QueryContinuationClause();
							continuation.PrecedingQuery = Detach(innerQuery);
							continuation.Identifier = fromClause.Identifier;
							fromClause.ReplaceWith(continuation);
						}
					}
					else
					{
						Match m = castPattern.Match(fromClause.Expression);
						if (m.Success)
						{
							fromClause.Type = Detach(m.Get<AstType>("targetType").Single());
							fromClause.Expression = Detach(m.Get<Expression>("inExpr").Single());
						}
					}
				}
			}

			static readonly QuerySelectClause selectTransparentIdentifierPattern = new QuerySelectClause
			{
				Expression = new Choice {
				new AnonymousTypeCreateExpression {
					Initializers = {
						new NamedNode("nae1", new NamedExpression {
							Name = Pattern.AnyString,
							Expression = new IdentifierExpression(Pattern.AnyString)
						}),
						new NamedNode("nae2", new NamedExpression {
							Name = Pattern.AnyString,
							Expression = new AnyNode("nae2Expr")
						})
					}
				},
				new AnonymousTypeCreateExpression {
					Initializers = {
						new NamedNode("identifier", new IdentifierExpression(Pattern.AnyString)),
						new AnyNode("nae2Expr")
					}
				}
			}
			};

			bool IsTransparentIdentifier(string identifier)
			{
				return identifier.Contains("h__TransparentIdentifier");
			}

			bool TryRemoveTransparentIdentifier(QueryExpression query, QueryFromClause fromClause, QueryExpression innerQuery)
			{
				if (!IsTransparentIdentifier(fromClause.Identifier))
					return false;
				Match match = selectTransparentIdentifierPattern.Match(innerQuery.Clauses.Last());
				if (!match.Success)
					return false;
				QuerySelectClause selectClause = (QuerySelectClause)innerQuery.Clauses.Last();
				NamedExpression nae1 = match.Get<NamedExpression>("nae1").SingleOrDefault();
				NamedExpression nae2 = match.Get<NamedExpression>("nae2").SingleOrDefault();
				if (nae1 != null && nae1.Name != ((IdentifierExpression)nae1.Expression).Identifier)
					return false;
				Expression nae2Expr = match.Get<Expression>("nae2Expr").Single();
				IdentifierExpression nae2IdentExpr = nae2Expr as IdentifierExpression;
				if (nae2IdentExpr != null && (nae2 == null || nae2.Name == nae2IdentExpr.Identifier))
				{
					// from * in (from x in ... select new { x = x, y = y }) ...
					// =>
					// from x in ... ...
					fromClause.Remove();
					selectClause.Remove();
					// Move clauses from innerQuery to query
					QueryClause insertionPos = null;
					foreach (var clause in innerQuery.Clauses)
					{
						query.Clauses.InsertAfter(insertionPos, insertionPos = Detach(clause));
					}
				}
				else
				{
					// from * in (from x in ... select new { x = x, y = expr }) ...
					// =>
					// from x in ... let y = expr ...
					fromClause.Remove();
					selectClause.Remove();
					// Move clauses from innerQuery to query
					QueryClause insertionPos = null;
					foreach (var clause in innerQuery.Clauses)
					{
						query.Clauses.InsertAfter(insertionPos, insertionPos = Detach(clause));
					}
					string ident;
					if (nae2 != null)
						ident = nae2.Name;
					else if (nae2Expr is IdentifierExpression)
						ident = ((IdentifierExpression)nae2Expr).Identifier;
					else if (nae2Expr is MemberReferenceExpression)
						ident = ((MemberReferenceExpression)nae2Expr).MemberName;
					else
						throw new InvalidOperationException("Could not infer name from initializer in AnonymousTypeCreateExpression");
					query.Clauses.InsertAfter(insertionPos, new QueryLetClause { Identifier = ident, Expression = Detach(nae2Expr) });
				}
				return true;
			}

			/// <summary>
			/// Removes all occurrences of transparent identifiers
			/// </summary>
			void RemoveTransparentIdentifierReferences(AstNode node)
			{
				foreach (AstNode child in node.Children)
				{
					RemoveTransparentIdentifierReferences(child);
				}
				MemberReferenceExpression mre = node as MemberReferenceExpression;
				if (mre != null)
				{
					IdentifierExpression ident = mre.Target as IdentifierExpression;
					if (ident != null && IsTransparentIdentifier(ident.Identifier))
					{
						IdentifierExpression newIdent = new IdentifierExpression(mre.MemberName);
						mre.TypeArguments.MoveTo(newIdent.TypeArguments);
						mre.ReplaceWith(newIdent);
						return;
					}
				}
			}

			private T Detach<T>(T astNode)
				where T : AstNode
			{
				astNode.Remove();
				return astNode;
			}
		}

		#endregion
	}
}