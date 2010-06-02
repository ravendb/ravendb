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
	public static class QueryParsingUtils
	{
		public static string ToCSharp(this Expression expression)
		{
			var output = new CSharpOutputVisitor();
			expression.AcceptVisitor(output, null);
			return (output.Text);
		}

		public static string GenerateText(TypeDeclaration type)
		{
			var unit = new CompilationUnit();
			unit.AddChild(new Using(typeof (AbstractViewGenerator).Namespace));
			unit.AddChild(new Using(typeof (Enumerable).Namespace));
			unit.AddChild(new Using(typeof (int).Namespace));
			unit.AddChild(new Using(typeof (LinqOnDynamic).Namespace));
			unit.AddChild(type);

			var output = new CSharpOutputVisitor();
			unit.AcceptVisitor(output, null);

			return output.Text;
		}

		public static VariableDeclaration GetVariableDeclarationForLinqQuery(string query)
		{
			var parser = ParserFactory.CreateParser(SupportedLanguage.CSharp, new StringReader("var q = " + query));

			var block = parser.ParseBlock();

			if (block.Children.Count != 1)
				throw new InvalidOperationException("Could not understand query: \r\n" + parser.Errors.ErrorOutput);

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
			if (selectClause == null)
				throw new InvalidOperationException("Variable initializer must be a select query expression");

			var createExpression = selectClause.Projection as ObjectCreateExpression;
			if (createExpression == null || createExpression.IsAnonymousType == false)
				throw new InvalidOperationException(
					"Variable initializer must be a select query expression returning an anonymous object");

			return variable;
		}

		public static VariableDeclaration GetVariableDeclarationForLinqMethods(string query)
		{
			var parser = ParserFactory.CreateParser(SupportedLanguage.CSharp, new StringReader("var q = " + query));

			var block = parser.ParseBlock();

			if (block.Children.Count != 1)
				throw new InvalidOperationException("Could not understand query: \r\n" + parser.Errors.ErrorOutput);

			var declaration = block.Children[0] as LocalVariableDeclaration;
			if (declaration == null)
				throw new InvalidOperationException("Only local variable decleration are allowed");

			if (declaration.Variables.Count != 1)
				throw new InvalidOperationException("Only one variable declaration is allowed");

			var variable = declaration.Variables[0];

			if (variable.Initializer as InvocationExpression == null)
				throw new InvalidOperationException("Variable declaration must have an initializer which is a method invocation expression");

			var targetObject = ((InvocationExpression) variable.Initializer).TargetObject as MemberReferenceExpression;
			if(targetObject == null)
				throw new InvalidOperationException("Variable initializer must be invoked on a method reference expression");

			if (targetObject.MemberName != "Select" && targetObject.MemberName != "SelectMany")
				throw new InvalidOperationException("Variable initializer must end with a select call");

			var lambdaExpression = ((InvocationExpression)variable.Initializer).Arguments.Last() as LambdaExpression;
			if (lambdaExpression == null)
				throw new InvalidOperationException("Variable initializer select must have a lambda expression");

			var objectCreateExpression = lambdaExpression.ExpressionBody as ObjectCreateExpression;
			if (objectCreateExpression == null)
				throw new InvalidOperationException("Variable initializer select must have a lambda expression with an object create expression");

			if (objectCreateExpression.IsAnonymousType == false)
				throw new InvalidOperationException("Variable initializer select must have a lambda expression creating an anoynmous type");
			
			return variable;
		}


		public static Type Compile(string fileName, string name, string queryText)
		{
			var provider = new CSharpCodeProvider(new Dictionary<string, string> {{"CompilerVersion", "v4.0"}});
			var results = provider.CompileAssemblyFromFile(new CompilerParameters
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
			}, fileName);

			if (results.Errors.HasErrors)
			{
				var sb = new StringBuilder()
					.AppendLine("Source code:")
					.AppendLine(queryText)
					.AppendLine();
				foreach (CompilerError error in results.Errors)
				{
					sb.AppendLine(error.ToString());
				}
				throw new InvalidOperationException(sb.ToString());
			}
			return results.CompiledAssembly.GetType(name);
		}
	}
}