using System;
using System.Text;
using Jint.Expressions;

namespace Jint
{
	public class JsCodeVisitor : IStatementVisitor
	{
		public StringBuilder Builder = new StringBuilder();
		private int indent;

		private void Indent()
		{
			for (int i = 0; i < indent; i++)
			{
				Builder.Append(' ');
			}
		}

		public void Visit(Program expression)
		{
			foreach (var statement in expression.Statements)
			{
				statement.Accept(this);
			}
		}

		public void Visit(AssignmentExpression expression)
		{
			expression.Left.Accept(this);
			Builder.Append(" = ");
			expression.Right.Accept(this);
		}

		public void Visit(BlockStatement expression)
		{
			Builder.AppendLine("{");
			indent++;
			foreach (var statement in expression.Statements)
			{
				Indent();
				statement.Accept(this);
				Builder.AppendLine();
			}
			indent--;
			Indent();
			Builder.AppendLine("}");
		}

		public void Visit(BreakStatement expression)
		{
			Builder.Append("break;");
		}

		public void Visit(ContinueStatement expression)
		{
			Builder.Append("continue;");
		}

		public void Visit(DoWhileStatement expression)
		{
			Builder.AppendLine("do {");
			indent++;
			Indent();
			expression.Statement.Accept(this);
			indent--;
			Indent();
			Builder.AppendLine().Append("} while (");
			expression.Condition.Accept(this);
			Builder.AppendLine(" );");

		}

		public void Visit(EmptyStatement expression)
		{

		}

		public void Visit(ExpressionStatement expression)
		{
			Indent();
			expression.Expression.Accept(this);
			Builder.AppendLine(";");
		}

		public void Visit(ForEachInStatement expression)
		{
			throw new System.NotImplementedException();
		}

		public void Visit(ForStatement expression)
		{
			Builder.Append("for (");
			expression.InitialisationStatement.Accept(this);
			Builder.Append("; ");
			expression.ConditionExpression.Accept(this);
			Builder.Append("; ");
			expression.IncrementExpression.Accept(this);
			Builder.AppendLine(") {");
			indent++;
			Indent();
			expression.Statement.Accept(this);
			indent--;
			Indent();
			Builder.AppendLine("}");

		}

		public void Visit(FunctionDeclarationStatement expression)
		{
			Builder.Append("function ")
				.Append(expression.Name)
				.Append("(");

			bool first = true;
			foreach (var parameter in expression.Parameters)
			{
				if (first == false)
					Builder.Append(", ");
				first = false;
				Builder.Append(parameter);
			}
			Builder.Append(") {");
			indent++;
			Indent();
			expression.Statement.Accept(this);
			indent--;
			Indent();
			Builder.AppendLine("}");
		}

		public void Visit(IfStatement expression)
		{
			Builder.Append("if (");
			expression.Expression.Accept(this);
			Builder.AppendLine(") {");
			indent++;
			Indent();
			expression.Then.Accept(this);
			indent--;
			Builder.Append("}");
			if (expression.Else == null)
			{
				Builder.AppendLine();
				return;
			}

			Builder.AppendLine("else {");
			indent++;
			Indent();
			expression.Then.Accept(this);
			indent--;
			Indent();
			Builder.AppendLine("}");
		}

		public void Visit(ReturnStatement expression)
		{
			Builder.Append("return ");
			expression.Expression.Accept(this);
			Builder.AppendLine(";");
		}

		public void Visit(SwitchStatement expression)
		{
			throw new System.NotImplementedException();
		}

		public void Visit(WithStatement expression)
		{
			throw new System.NotImplementedException();
		}

		public void Visit(ThrowStatement expression)
		{
			Builder.Append("throw ");
			expression.Expression.Accept(this);
			Builder.AppendLine(";");
		}

		public void Visit(TryStatement expression)
		{
			throw new System.NotImplementedException();
		}

		public void Visit(VariableDeclarationStatement expression)
		{
			Builder.Append("var ")
				.Append(expression.Identifier);
			if (expression.Expression == null)
			{
				Builder.AppendLine(";");
				return;
			}
			Builder.Append(" = ");
			expression.Expression.Accept(this);
			Builder.AppendLine(";");
		}

		public void Visit(WhileStatement expression)
		{
			Builder.Append("while (");
			expression.Condition.Accept(this);
			Builder.AppendLine(") {");
			indent++;
			Indent();
			expression.Statement.Accept(this);
			indent--;
			Indent();
			Builder.AppendLine("}");
		}

		public void Visit(ArrayDeclaration expression)
		{
			indent++;
			foreach (var parameter in expression.Parameters)
			{
				Indent();
				parameter.Accept(this);
				Builder.AppendLine();
			}
			indent--;
		}

		public void Visit(CommaOperatorStatement expression)
		{
			var first = true;
			foreach (var parameter in expression.Statements)
			{
				if (first == false)
					Builder.Append(", ");
				first = false;
				parameter.Accept(this);
			}
		}

		public void Visit(FunctionExpression expression)
		{
			Builder.Append("function ")
				.Append(expression.Name)
				.Append("(");

			bool first = true;
			foreach (var parameter in expression.Parameters)
			{
				if (first == false)
					Builder.Append(", ");
				first = false;
				Builder.Append(parameter);
			}
			Builder.Append(") {");
			indent++;
			Indent();
			expression.Statement.Accept(this);
			indent--;
			Indent();
			Builder.AppendLine("}");
		}

		public void Visit(MemberExpression expression)
		{
			if (expression.Previous != null)
			{
				expression.Previous.Accept(this);
				Builder.Append(".");
			}
			expression.Member.Accept(this);
		}

		public void Visit(MethodCall expression)
		{
			Builder.Append("(");
			var first = true;
			foreach (var parameter in expression.Arguments)
			{
				if (first == false)
					Builder.Append(", ");
				first = false;
				parameter.Accept(this);
			}
			Builder.Append(")");
		}

		public void Visit(Indexer expression)
		{
			Builder.Append("[");
			expression.Index.Accept(this);
			Builder.Append("]");
		}

		public void Visit(PropertyExpression expression)
		{
			Builder.Append(expression.Text);
		}

		public void Visit(PropertyDeclarationExpression expression)
		{
			throw new System.NotImplementedException();
		}

		public void Visit(Identifier expression)
		{
			Builder.Append(expression.Text);
		}

		public void Visit(JsonExpression expression)
		{
			Builder.Append("{");
			indent++;

			foreach (var value in expression.Values)
			{
				Indent();
				Builder.Append("\"").Append(value.Key).Append("\": ");
				value.Value.Accept(this);
			}

			indent--;
			Indent();
			Builder.Append("}");
		}

		public void Visit(NewExpression expression)
		{
			Builder.Append("new ");
			expression.Expression.Accept(this);
			Builder.Append("(");
			var first = true;
			foreach (var argument in expression.Arguments)
			{
				if (first == false)
					Builder.Append(", ");
				first = false;
				argument.Accept(this);
			}
			Builder.Append(")");
		}

		public void Visit(BinaryExpression expression)
		{
			expression.LeftExpression.Accept(this);
			Builder.Append(' ');
			switch (expression.Type)
			{
				case BinaryExpressionType.And:
					Builder.Append("&&");
					break;
				case BinaryExpressionType.Or:
					Builder.Append("||");
					break;
				case BinaryExpressionType.NotEqual:
					Builder.Append("!=");
					break;
				case BinaryExpressionType.LesserOrEqual:
					Builder.Append("<=");
					break;
				case BinaryExpressionType.GreaterOrEqual:
					Builder.Append(">=");
					break;
				case BinaryExpressionType.Lesser:
					Builder.Append("<");
					break;
				case BinaryExpressionType.Greater:
					Builder.Append(">");
					break;
				case BinaryExpressionType.Equal:
					Builder.Append("");
					break;
				case BinaryExpressionType.Minus:
					Builder.Append("-");
					break;
				case BinaryExpressionType.Plus:
					Builder.Append("+");
					break;
				case BinaryExpressionType.Modulo:
					Builder.Append("%");
					break;
				case BinaryExpressionType.Div:
					Builder.Append("/");
					break;
				case BinaryExpressionType.Times:
					Builder.Append("*");
					break;
				case BinaryExpressionType.Pow:
					Builder.Append("**");
					break;
				case BinaryExpressionType.BitwiseAnd:
					Builder.Append("&");
					break;
				case BinaryExpressionType.BitwiseOr:
					Builder.Append("|");
					break;
				case BinaryExpressionType.BitwiseXOr:
					Builder.Append("^");
					break;
				case BinaryExpressionType.Same:
					Builder.Append("===");
					break;
				case BinaryExpressionType.NotSame:
					Builder.Append("!===");
					break;
				case BinaryExpressionType.LeftShift:
					Builder.Append("<<");
					break;
				case BinaryExpressionType.RightShift:
					Builder.Append(">>");
					break;
				case BinaryExpressionType.UnsignedRightShift:
					Builder.Append(">>>");
					break;
				case BinaryExpressionType.InstanceOf:
					Builder.Append("instanceof");
					break;
				case BinaryExpressionType.In:
					Builder.Append("in");
					break;
				case BinaryExpressionType.Unknown:
					Builder.Append(" /* unknown */");
					break;
				default:
					throw new ArgumentOutOfRangeException();
			}
			Builder.Append(' ');
			expression.RightExpression.Accept(this);
		}

		public void Visit(TernaryExpression expression)
		{
			expression.LeftExpression.Accept(this);
			Builder.Append(" ? ");
			expression.MiddleExpression.Accept(this);
			Builder.Append(" : ");
			expression.RightExpression.Accept(this);
		}

		public void Visit(UnaryExpression expression)
		{
			switch (expression.Type)
			{
				case UnaryExpressionType.TypeOf:
					Builder.Append("typeof ");
					break;
				case UnaryExpressionType.New:
					Builder.Append("new ");
					break;
				case UnaryExpressionType.Not:
					Builder.Append("!");
					break;
				case UnaryExpressionType.Negate:
					Builder.Append("-");
					break;
				case UnaryExpressionType.Positive:
					Builder.Append("+");
					break;
				case UnaryExpressionType.PrefixPlusPlus:
					Builder.Append("++");
					break;
				case UnaryExpressionType.PrefixMinusMinus:
					Builder.Append("--");
					break;
				case UnaryExpressionType.Delete:
					Builder.Append("delete ");
					break;

			}
			expression.Expression.Accept(this);
			switch (expression.Type)
			{
				case UnaryExpressionType.PostfixPlusPlus:
					Builder.Append("++");
					break;
				case UnaryExpressionType.PostfixMinusMinus:
					Builder.Append("--");
					break;
				//case UnaryExpressionType.Void:
				//case UnaryExpressionType.Inv:
				//case UnaryExpressionType.Unknown:
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		public void Visit(ValueExpression expression)
		{
			Builder.Append(expression.Value);
		}

		public void Visit(RegexpExpression expression)
		{
			Builder
				.Append("/")
				.Append(expression.Regexp)
				.Append("/")
				.Append(expression.Options);
		}

		public void Visit(Statement expression)
		{
			throw new System.NotSupportedException();
		}
	}
}