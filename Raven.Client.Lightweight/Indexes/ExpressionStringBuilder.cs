// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
#if !NET35
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Client.Indexes
{
	/// <summary>
	///   Based off of System.Linq.Expressions.ExpressionStringBuilder
	/// </summary>
	public class ExpressionStringBuilder : ExpressionVisitor
	{
		// Fields
		private readonly StringBuilder _out = new StringBuilder();
		private readonly DocumentConvention convention;
		private readonly Type queryRoot;
		private readonly string queryRootName;
		private readonly bool translateIdentityProperty;
		private ExpressionOperatorPrecedence _currentPrecedence;
		private Dictionary<object, int> _ids;
		private bool castLambdas;

		// Methods
		private ExpressionStringBuilder(DocumentConvention convention, bool translateIdentityProperty, Type queryRoot,
										string queryRootName)
		{
			this.convention = convention;
			this.translateIdentityProperty = translateIdentityProperty;
			this.queryRoot = queryRoot;
			this.queryRootName = queryRootName;
		}

		private void AddLabel(LabelTarget label)
		{
			if (_ids == null)
			{
				_ids = new Dictionary<object, int> { { label, 0 } };
			}
			else if (!_ids.ContainsKey(label))
			{
				_ids.Add(label, _ids.Count);
			}
		}

		private void AddParam(ParameterExpression p)
		{
			if (_ids == null)
			{
				_ids = new Dictionary<object, int>();
				_ids.Add(_ids, 0);
			}
			else if (!_ids.ContainsKey(p))
			{
				_ids.Add(p, _ids.Count);
			}
		}

		internal string CatchBlockToString(CatchBlock node)
		{
			var builder = new ExpressionStringBuilder(convention, translateIdentityProperty, queryRoot, queryRootName);
			builder.VisitCatchBlock(node);
			return builder.ToString();
		}

		private void DumpLabel(LabelTarget target)
		{
			if (!string.IsNullOrEmpty(target.Name))
			{
				Out(target.Name);
			}
			else
			{
				Out("UnamedLabel_" + GetLabelId(target));
			}
		}

		internal string ElementInitBindingToString(ElementInit node)
		{
			var builder = new ExpressionStringBuilder(convention, translateIdentityProperty, queryRoot, queryRootName);
			builder.VisitElementInit(node);
			return builder.ToString();
		}

		/// <summary>
		///   Convert the expression to a string
		/// </summary>
		public static string ExpressionToString(DocumentConvention convention, bool translateIdentityProperty, Type queryRoot,
												string queryRootName, Expression node)
		{
			var builder = new ExpressionStringBuilder(convention, translateIdentityProperty, queryRoot, queryRootName);
			builder.Visit(node, ExpressionOperatorPrecedence.ParenthesisNotNeeded);
			return builder.ToString();
		}

		private static string FormatBinder(CallSiteBinder binder)
		{
			var binder2 = binder as ConvertBinder;
			if (binder2 != null)
			{
				return ("Convert " + binder2.Type);
			}
			var binder3 = binder as GetMemberBinder;
			if (binder3 != null)
			{
				return ("GetMember " + binder3.Name);
			}
			var binder4 = binder as SetMemberBinder;
			if (binder4 != null)
			{
				return ("SetMember " + binder4.Name);
			}
			var binder5 = binder as DeleteMemberBinder;
			if (binder5 != null)
			{
				return ("DeleteMember " + binder5.Name);
			}
			if (binder is GetIndexBinder)
			{
				return "GetIndex";
			}
			if (binder is SetIndexBinder)
			{
				return "SetIndex";
			}
			if (binder is DeleteIndexBinder)
			{
				return "DeleteIndex";
			}
			var binder6 = binder as InvokeMemberBinder;
			if (binder6 != null)
			{
				return ("Call " + binder6.Name);
			}
			if (binder is InvokeBinder)
			{
				return "Invoke";
			}
			if (binder is CreateInstanceBinder)
			{
				return "Create";
			}
			var binder7 = binder as UnaryOperationBinder;
			if (binder7 != null)
			{
				return binder7.Operation.ToString();
			}
			var binder8 = binder as BinaryOperationBinder;
			if (binder8 != null)
			{
				return binder8.Operation.ToString();
			}
			return "CallSiteBinder";
		}

		private int GetLabelId(LabelTarget label)
		{
			int count;
			if (_ids == null)
			{
				_ids = new Dictionary<object, int>();
				AddLabel(label);
				return 0;
			}
			if (!_ids.TryGetValue(label, out count))
			{
				count = _ids.Count;
				AddLabel(label);
			}
			return count;
		}

		private int GetParamId(ParameterExpression p)
		{
			int count;
			if (_ids == null)
			{
				_ids = new Dictionary<object, int>();
				AddParam(p);
				return 0;
			}
			if (!_ids.TryGetValue(p, out count))
			{
				count = _ids.Count;
				AddParam(p);
			}
			return count;
		}

		internal string MemberBindingToString(MemberBinding node)
		{
			var builder = new ExpressionStringBuilder(convention, translateIdentityProperty, queryRoot, queryRootName);
			builder.VisitMemberBinding(node);
			return builder.ToString();
		}

		private void Out(char c)
		{
			_out.Append(c);
		}

		private void Out(string s)
		{
			_out.Append(s);
		}

		private void OutMember(Expression instance, MemberInfo member, Type exprType)
		{
			if (instance == null || instance.NodeType != ExpressionType.MemberAccess)
			{
				OutputTypeIfNeeded(member);
			}
			var name = member.Name;
			if (translateIdentityProperty &&
				convention.GetIdentityProperty(member.DeclaringType) == member &&
				// only translate from the root type or deriatives
				(queryRoot == null || (exprType.IsAssignableFrom(queryRoot))) &&
				// only translate from the root alias
				(queryRootName == null || (
					instance.NodeType == ExpressionType.Parameter &&
					((ParameterExpression)instance).Name == queryRootName)))
			{
				name = Constants.DocumentIdFieldName;
			}
			if (instance != null)
			{
				Visit(instance);
				Out("." + name);
			}
			else
			{
				var parentType = member.DeclaringType;
				while (parentType.IsNested)
				{
					parentType = parentType.DeclaringType;
					if (parentType == null)
						break;
					Out(parentType.Name + ".");
				}

				Out(member.DeclaringType.Name + "." + name);
			}
			if (instance == null || instance.NodeType != ExpressionType.MemberAccess)
			{
				CloseOutputTypeIfNeeded(member);
			}
		}

		private void CloseOutputTypeIfNeeded(MemberInfo member)
		{
			var memberType = GetMemberType(member);
			if (memberType == typeof(decimal) ||
				memberType == typeof(double) ||
				memberType == typeof(long) ||
				memberType == typeof(float) ||
				memberType == typeof(decimal?) ||
				memberType == typeof(double?) ||
				memberType == typeof(long?) ||
				memberType == typeof(float?))
			{
				Out(")");
			}
		}

		private void OutputTypeIfNeeded(MemberInfo member)
		{
			var memberType = GetMemberType(member);
			if (memberType == typeof(decimal))
			{
				Out("((decimal)");
			}
			if (memberType == typeof(double))
			{
				Out("((double)");
			}
			if (memberType == typeof(long))
			{
				Out("((long)");
			}
			if (memberType == typeof(float))
			{
				Out("((float)");
			}
			if (memberType == typeof(decimal?))
			{
				Out("((decimal?)");
			}
			if (memberType == typeof(double?))
			{
				Out("((double?)");
			}
			if (memberType == typeof(long?))
			{
				Out("((long?)");
			}
			if (memberType == typeof(float?))
			{
				Out("((float?)");
			}
		}

		private static Type GetMemberType(MemberInfo member)
		{
			var prop = member as PropertyInfo;
			if (prop != null)
				return prop.PropertyType;
			return ((FieldInfo)member).FieldType;
		}

		internal string SwitchCaseToString(SwitchCase node)
		{
			var builder = new ExpressionStringBuilder(convention, translateIdentityProperty, queryRoot, queryRootName);
			builder.VisitSwitchCase(node);
			return builder.ToString();
		}

		/// <summary>
		///   Returns a <see cref = "System.String" /> that represents this instance.
		/// </summary>
		/// <returns>
		///   A <see cref = "System.String" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			return _out.ToString();
		}

		private void SometimesParenthesis(ExpressionOperatorPrecedence outer, ExpressionOperatorPrecedence inner,
										  Action visitor)
		{
			var needParenthesis = outer.NeedsParenthesisFor(inner);

			if (needParenthesis)
				Out("(");

			visitor();

			if (needParenthesis)
				Out(")");
		}

		private void Visit(Expression node, ExpressionOperatorPrecedence outerPrecedence)
		{
			var previous = _currentPrecedence;
			_currentPrecedence = outerPrecedence;
			Visit(node);
			_currentPrecedence = previous;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.BinaryExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitBinary(BinaryExpression node)
		{
			return VisitBinary(node, _currentPrecedence);
		}

		private Expression VisitBinary(BinaryExpression node, ExpressionOperatorPrecedence outerPrecedence)
		{
			ExpressionOperatorPrecedence innerPrecedence;

			string str;
			var leftOp = node.Left;
			var rightOp = node.Right;

			FixupEnumBinaryExpression(ref leftOp, ref rightOp);
			switch (node.NodeType)
			{
				case ExpressionType.Add:
					str = "+";
					innerPrecedence = ExpressionOperatorPrecedence.Additive;
					break;

				case ExpressionType.AddChecked:
					str = "+";
					innerPrecedence = ExpressionOperatorPrecedence.Additive;
					break;

				case ExpressionType.And:
					if ((node.Type != typeof(bool)) && (node.Type != typeof(bool?)))
					{
						str = "&";
						innerPrecedence = ExpressionOperatorPrecedence.LogicalAND;
					}
					else
					{
						str = "And";
						innerPrecedence = ExpressionOperatorPrecedence.ConditionalAND;
					}
					break;

				case ExpressionType.AndAlso:
					str = "&&";
					innerPrecedence = ExpressionOperatorPrecedence.ConditionalAND;
					break;

				case ExpressionType.Coalesce:
					str = "??";
					innerPrecedence = ExpressionOperatorPrecedence.NullCoalescing;
					break;

				case ExpressionType.Divide:
					str = "/";
					innerPrecedence = ExpressionOperatorPrecedence.Multiplicative;
					break;

				case ExpressionType.Equal:
					str = "==";
					innerPrecedence = ExpressionOperatorPrecedence.Equality;
					break;

				case ExpressionType.ExclusiveOr:
					str = "^";
					innerPrecedence = ExpressionOperatorPrecedence.LogicalXOR;
					break;

				case ExpressionType.GreaterThan:
					str = ">";
					innerPrecedence = ExpressionOperatorPrecedence.RelationalAndTypeTesting;
					break;

				case ExpressionType.GreaterThanOrEqual:
					str = ">=";
					innerPrecedence = ExpressionOperatorPrecedence.RelationalAndTypeTesting;
					break;

				case ExpressionType.LeftShift:
					str = "<<";
					innerPrecedence = ExpressionOperatorPrecedence.Shift;
					break;

				case ExpressionType.LessThan:
					str = "<";
					innerPrecedence = ExpressionOperatorPrecedence.RelationalAndTypeTesting;
					break;

				case ExpressionType.LessThanOrEqual:
					str = "<=";
					innerPrecedence = ExpressionOperatorPrecedence.RelationalAndTypeTesting;
					break;

				case ExpressionType.Modulo:
					str = "%";
					innerPrecedence = ExpressionOperatorPrecedence.Multiplicative;
					break;

				case ExpressionType.Multiply:
					str = "*";
					innerPrecedence = ExpressionOperatorPrecedence.Multiplicative;
					break;

				case ExpressionType.MultiplyChecked:
					str = "*";
					innerPrecedence = ExpressionOperatorPrecedence.Multiplicative;
					break;

				case ExpressionType.NotEqual:
					str = "!=";
					innerPrecedence = ExpressionOperatorPrecedence.RelationalAndTypeTesting;
					break;

				case ExpressionType.Or:
					if ((node.Type != typeof(bool)) && (node.Type != typeof(bool?)))
					{
						str = "|";
						innerPrecedence = ExpressionOperatorPrecedence.LogicalOR;
					}
					else
					{
						str = "Or";
						innerPrecedence = ExpressionOperatorPrecedence.LogicalOR;
					}
					break;

				case ExpressionType.OrElse:
					str = "||";
					innerPrecedence = ExpressionOperatorPrecedence.ConditionalOR;
					break;

				case ExpressionType.Power:
					str = "^";
					innerPrecedence = ExpressionOperatorPrecedence.LogicalXOR;
					break;

				case ExpressionType.RightShift:
					str = ">>";
					innerPrecedence = ExpressionOperatorPrecedence.Shift;
					break;

				case ExpressionType.Subtract:
					str = "-";
					innerPrecedence = ExpressionOperatorPrecedence.Additive;
					break;

				case ExpressionType.SubtractChecked:
					str = "-";
					innerPrecedence = ExpressionOperatorPrecedence.Additive;
					break;

				case ExpressionType.Assign:
					str = "=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.AddAssign:
					str = "+=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.AndAssign:
					if ((node.Type != typeof(bool)) && (node.Type != typeof(bool?)))
					{
						str = "&=";
						innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					}
					else
					{
						str = "&&=";
						innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					}
					break;

				case ExpressionType.DivideAssign:
					str = "/=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.ExclusiveOrAssign:
					str = "^=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.LeftShiftAssign:
					str = "<<=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.ModuloAssign:
					str = "%=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.MultiplyAssign:
					str = "*=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.OrAssign:
					if ((node.Type != typeof(bool)) && (node.Type != typeof(bool?)))
					{
						str = "|=";
						innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					}
					else
					{
						str = "||=";
						innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					}
					break;

				case ExpressionType.PowerAssign:
					str = "**=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.RightShiftAssign:
					str = ">>=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.SubtractAssign:
					str = "-=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.AddAssignChecked:
					str = "+=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.MultiplyAssignChecked:
					str = "*=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.SubtractAssignChecked:
					str = "-=";
					innerPrecedence = ExpressionOperatorPrecedence.Assignment;
					break;

				case ExpressionType.ArrayIndex:

					innerPrecedence = ExpressionOperatorPrecedence.Primary;

					SometimesParenthesis(outerPrecedence, innerPrecedence, delegate
					{
						Visit(leftOp, innerPrecedence);
						Out("[");
						Visit(rightOp, innerPrecedence);
						Out("]");
					});
					return node;

				default:
					throw new InvalidOperationException();
			}


			SometimesParenthesis(outerPrecedence, innerPrecedence, delegate
			{
				Visit(leftOp, innerPrecedence);
				Out(' ');
				Out(str);
				Out(' ');
				Visit(rightOp, innerPrecedence);
			});

			return node;
		}

		private void FixupEnumBinaryExpression(ref Expression left, ref Expression right)
		{
			switch (left.NodeType)
			{
				case ExpressionType.ConvertChecked:
				case ExpressionType.Convert:
					var expression = ((UnaryExpression)left).Operand;
					if (expression.Type.IsEnum == false)
						return;
					var constantExpression = right as ConstantExpression;
					if (constantExpression == null)
						return;
					left = expression;
					right = convention.SaveEnumsAsIntegers ?
						Expression.Constant((int)constantExpression.Value) :
						Expression.Constant(Enum.ToObject(expression.Type, constantExpression.Value).ToString());
					break;
			}

			while (true)
			{
				switch (left.NodeType)
				{
					case ExpressionType.ConvertChecked:
					case ExpressionType.Convert:
						left = ((UnaryExpression)left).Operand;
						break;
					default:
						return;
				}
			}
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.BlockExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitBlock(BlockExpression node)
		{
			Out("{");
			foreach (var expression in node.Variables)
			{
				Out("var ");
				Visit(expression);
				Out(";");
			}
			Out(" ... }");
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.CatchBlock" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override CatchBlock VisitCatchBlock(CatchBlock node)
		{
			Out("catch (" + node.Test.Name);
			if (node.Variable != null)
			{
				Out(node.Variable.Name);
			}
			Out(") { ... }");
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.ConditionalExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitConditional(ConditionalExpression node)
		{
			return VisitConditional(node, _currentPrecedence);
		}

		private Expression VisitConditional(ConditionalExpression node, ExpressionOperatorPrecedence outerPrecedence)
		{
			const ExpressionOperatorPrecedence innerPrecedence = ExpressionOperatorPrecedence.Conditional;

			SometimesParenthesis(outerPrecedence, innerPrecedence, delegate
			{
				Visit(node.Test, innerPrecedence);
				Out(" ? ");
				Visit(node.IfTrue, innerPrecedence);
				Out(" : ");
				Visit(node.IfFalse, innerPrecedence);
			});

			return node;
		}

		/// <summary>
		///   Visits the <see cref = "T:System.Linq.Expressions.ConstantExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitConstant(ConstantExpression node)
		{
			if (node.Value != null)
			{
				var s = Convert.ToString(node.Value, CultureInfo.InvariantCulture);
				if (node.Value is string)
				{
					Out("\"");
					Out(s);
					Out("\"");
					return node;
				}
				if (node.Value is bool)
				{
					Out(node.Value.ToString().ToLower());
					return node;
				}
				if (node.Value is char)
				{
					Out("'");
					Out(s);
					Out("'");
					return node;
				}
				if (node.Value is Enum)
				{
					var enumType = node.Value.GetType();
					if (TypeExistsOnServer(enumType))
					{
						Out(enumType.FullName.Replace("+", "."));
						Out('.');
						Out(s);
						return node;
					}
					Out('"');
					Out(node.Value.ToString());
					Out('"');
					return node;
				}
				if (node.Value is decimal)
				{
					Out(s);
					Out('M');
					return node;
				}
				Out(s);
				return node;
			}
			Out("null");
			return node;
		}

		private bool TypeExistsOnServer(Type type)
		{
			if (type.Assembly == typeof(object).Assembly)
				return true;

			if (type.Assembly == typeof(RavenJObject).Assembly)
				return true;

			if (type.Assembly.FullName.StartsWith("Lucene.Net") &&
				type.Assembly.FullName.Contains("PublicKeyToken=85089178b9ac3181")) 
				return true;

			return false;
		}

		/// <summary>
		///   Visits the <see cref = "T:System.Linq.Expressions.DebugInfoExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitDebugInfo(DebugInfoExpression node)
		{
			var s = string.Format(CultureInfo.CurrentCulture, "<DebugInfo({0}: {1}, {2}, {3}, {4})>",
								  new object[] { node.Document.FileName, node.StartLine, node.StartColumn, node.EndLine, node.EndColumn });
			Out(s);
			return node;
		}

		/// <summary>
		///   Visits the <see cref = "T:System.Linq.Expressions.DefaultExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitDefault(DefaultExpression node)
		{
			Out("default(");
			Out(node.Type.Name);
			Out(")");
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.DynamicExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitDynamic(DynamicExpression node)
		{
			Out(FormatBinder(node.Binder));
			VisitExpressions('(', node.Arguments, ')');
			return node;
		}

		/// <summary>
		///   Visits the element init.
		/// </summary>
		/// <param name = "initializer">The initializer.</param>
		/// <returns></returns>
		protected override ElementInit VisitElementInit(ElementInit initializer)
		{
			Out(initializer.AddMethod.ToString());
			VisitExpressions('(', initializer.Arguments, ')');
			return initializer;
		}

		private void VisitExpressions<T>(char open, IEnumerable<T> expressions, char close) where T : Expression
		{
			Out(open);
			if (expressions != null)
			{
				var flag = true;
				foreach (var local in expressions)
				{
					if (flag)
					{
						flag = false;
					}
					else
					{
						Out(", ");
					}
					Visit(local, ExpressionOperatorPrecedence.ParenthesisNotNeeded);
				}
			}
			Out(close);
		}

		/// <summary>
		///   Visits the children of the extension expression.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitExtension(Expression node)
		{
			const BindingFlags bindingAttr = BindingFlags.ExactBinding | BindingFlags.Public | BindingFlags.Instance;
			if (node.GetType().GetMethod("ToString", bindingAttr, null, Type.EmptyTypes, null).DeclaringType !=
				typeof(Expression))
			{
				Out(node.ToString());
				return node;
			}
			Out("[");
			Out(node.NodeType == ExpressionType.Extension ? node.GetType().FullName : node.NodeType.ToString());
			Out("]");
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.GotoExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitGoto(GotoExpression node)
		{
			Out(node.Kind.ToString().ToLower(CultureInfo.CurrentCulture));
			DumpLabel(node.Target);
			if (node.Value != null)
			{
				Out(" (");
				Visit(node.Value);
				Out(") ");
			}
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.IndexExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitIndex(IndexExpression node)
		{
			if (node.Object != null)
			{
				Visit(node.Object);
			}
			else
			{
				Out(node.Indexer.DeclaringType.Name);
			}
			if (node.Indexer != null)
			{
				Out(".");
				Out(node.Indexer.Name);
			}
			VisitExpressions('[', node.Arguments, ']');
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.InvocationExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitInvocation(InvocationExpression node)
		{
			Out("Invoke(");
			Visit(node.Expression);
			var num = 0;
			var count = node.Arguments.Count;
			while (num < count)
			{
				Out(", ");
				Visit(node.Arguments[num]);
				num++;
			}
			Out(")");
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.LabelExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitLabel(LabelExpression node)
		{
			Out("{ ... } ");
			DumpLabel(node.Target);
			Out(":");
			return node;
		}

		/// <summary>
		///   Visits the lambda.
		/// </summary>
		/// <typeparam name = "T"></typeparam>
		/// <param name = "node">The node.</param>
		/// <returns></returns>
		protected override Expression VisitLambda<T>(Expression<T> node)
		{
			if (node.Parameters.Count == 1)
			{
				Visit(node.Parameters[0]);
			}
			else
			{
				VisitExpressions('(', node.Parameters, ')');
			}
			Out(" => ");
			var body = node.Body;
			if (castLambdas)
			{
				switch (body.NodeType)
				{
					case ExpressionType.Convert:
					case ExpressionType.ConvertChecked:
						break;
					default:
						body = Expression.Convert(body, body.Type);
						break;
				}
			}
			Visit(body);
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.ListInitExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitListInit(ListInitExpression node)
		{
			Visit(node.NewExpression);
			Out(" {");
			var num = 0;
			var count = node.Initializers.Count;
			while (num < count)
			{
				if (num > 0)
				{
					Out(", ");
				}
				Out("{");
				bool first = true;
				foreach (var expression in node.Initializers[num].Arguments)
				{
					if (first == false)
						Out(", ");
					first = false;
					Visit(expression);
				}
				Out("}");
				num++;
			}
			Out("}");
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.LoopExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitLoop(LoopExpression node)
		{
			Out("loop { ... }");
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.MemberExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitMember(MemberExpression node)
		{
			OutMember(node.Expression, node.Member, node.Expression == null ? node.Type : node.Expression.Type);
			return node;
		}

		/// <summary>
		///   Visits the member assignment.
		/// </summary>
		/// <param name = "assignment">The assignment.</param>
		/// <returns></returns>
		protected override MemberAssignment VisitMemberAssignment(MemberAssignment assignment)
		{
			Out(assignment.Member.Name);
			Out(" = ");
			Visit(assignment.Expression);
			return assignment;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.MemberInitExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitMemberInit(MemberInitExpression node)
		{
			if ((node.NewExpression.Arguments.Count == 0) && node.NewExpression.Type.Name.Contains("<"))
			{
				Out("new");
			}
			else
			{
				Visit(node.NewExpression);
			}
			Out(" {");
			var num = 0;
			var count = node.Bindings.Count;
			while (num < count)
			{
				var binding = node.Bindings[num];
				if (num > 0)
				{
					Out(", ");
				}
				VisitMemberBinding(binding);
				num++;
			}
			Out("}");
			return node;
		}

		/// <summary>
		///   Visits the member list binding.
		/// </summary>
		/// <param name = "binding">The binding.</param>
		/// <returns></returns>
		protected override MemberListBinding VisitMemberListBinding(MemberListBinding binding)
		{
			Out(binding.Member.Name);
			Out(" = {");
			var num = 0;
			var count = binding.Initializers.Count;
			while (num < count)
			{
				if (num > 0)
				{
					Out(", ");
				}
				VisitElementInit(binding.Initializers[num]);
				num++;
			}
			Out("}");
			return binding;
		}

		/// <summary>
		///   Visits the member member binding.
		/// </summary>
		/// <param name = "binding">The binding.</param>
		/// <returns></returns>
		protected override MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding binding)
		{
			Out(binding.Member.Name);
			Out(" = {");
			var num = 0;
			var count = binding.Bindings.Count;
			while (num < count)
			{
				if (num > 0)
				{
					Out(", ");
				}
				VisitMemberBinding(binding.Bindings[num]);
				num++;
			}
			Out("}");
			return binding;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.MethodCallExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitMethodCall(MethodCallExpression node)
		{
			var num = 0;
			var expression = node.Object;
			if (IsExtensionMethod(node))
			{
				num = 1;
				expression = node.Arguments[0];
			}
			if (expression != null)
			{
				switch (node.Method.Name)
				{
					case "MetadataFor":
						Visit(node.Arguments[0]);
						Out("[\"@metadata\"]");
						return node;
					case "AsDocument":
						Visit(node.Arguments[0]);
						return node;
				}
				if (expression.Type == typeof(IClientSideDatabase))
				{
					Out("Database");
				}
#if !SILVERLIGHT
				else if (typeof(AbstractIndexCreationTask).IsAssignableFrom(expression.Type))
				{
					// this is a method that
					// exists on both the server side and the client side
					Out("this");
				}
#endif
				else
				{
					Visit(expression);
				}
				if (node.Method.Name != "get_Item") // VB indexer
				{
					Out(".");
				}
			}
			if (node.Method.IsStatic && IsExtensionMethod(node) == false)
			{
				Out(node.Method.DeclaringType.Name);
				Out(".");
			}
			if (node.Method.Name != "get_Item") // VB indexer
			{
				Out(node.Method.Name);
				Out("(");
			}
			else
			{
				Out("[");
			}
			var num2 = num;
			var count = node.Arguments.Count;
			while (num2 < count)
			{
				if (num2 > num)
				{
					Out(", ");
				}
				var old = castLambdas;
				try
				{
					switch (node.Method.Name)
					{
						case "Sum":
						case "Average":
						case "Min":
						case "Max":
							castLambdas = true;
							break;
						default:
							castLambdas = false;
							break;
					}
					Visit(node.Arguments[num2]);
				}
				finally
				{
					castLambdas = old;
				}
				num2++;
			}
			Out(node.Method.Name != "get_Item" ? ")" : "]");
			return node;
		}

		private static bool IsExtensionMethod(MethodCallExpression node)
		{
			if (Attribute.GetCustomAttribute(node.Method, typeof(ExtensionAttribute)) == null)
				return false;

			if (node.Method.DeclaringType.Name == "Enumerable")
			{
				switch (node.Method.Name)
				{
					case "Select":
					case "SelectMany":
					case "Where":
					case "GroupBy":
					case "OrderBy":
					case "OrderByDescending":
					case "DefaultIfEmpty":
					case "First":
					case "FirstOrDefault":
					case "Single":
					case "SingleOrDefault":
					case "Last":
					case "LastOrDefault":
					case "Reverse":
						return true;
				}
				return false;
			}
			return true;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.NewExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitNew(NewExpression node)
		{
			Out("new ");
			VisitType(node.Type);
			Out("(");
			for (var i = 0; i < node.Arguments.Count; i++)
			{
				if (i > 0)
				{
					Out(", ");
				}
				if (node.Members != null && node.Members[i] != null)
				{
					Out(node.Members[i].Name);
					Out(" = ");

					var constantExpression = node.Arguments[i] as ConstantExpression;
					if (constantExpression != null && constantExpression.Value == null)
					{
						Out("(");
						VisitType(GetMemberType(node.Members[i]));
						Out(")");
					}
				}

				Visit(node.Arguments[i]);
			}
			Out(")");
			return node;
		}

		private static readonly Dictionary<Type, string> wellKnownTypes = new Dictionary<Type, string>
		{
			{typeof (object), "object"},
			{typeof (string), "string"},
			{typeof (int), "int"},
			{typeof (long), "long"},
			{typeof (float), "float"},
			{typeof (double), "double"},
			{typeof (decimal), "decimal"},
			{typeof (bool), "bool"},
			{typeof (char), "char"},
			{typeof (byte), "byte"},
			{typeof (Guid), "Guid"},
			{typeof (DateTime), "DateTime"},
			{typeof (DateTimeOffset), "DateTimeOffset"},
			{typeof (TimeSpan), "TimeSpan"},
		};
		private void VisitType(Type type)
		{
			if (type.IsGenericType == false || CheckIfAnonymousType(type))
			{
				if(type.IsArray)
				{
					VisitType(type.GetElementType());
					Out("[");
					for (int i = 0; i < type.GetArrayRank()-1; i++)
					{
						Out(",");
					}
					Out("]");
					return;
				}
				var nonNullableType = Nullable.GetUnderlyingType(type);
				if(nonNullableType != null)
				{
					VisitType(nonNullableType);
					Out("?");
					return;
				}
				string value;
				if(wellKnownTypes.TryGetValue(type, out value))
				{
					Out(value);
					return;
				}
				Out(type.Name);
				return;
			}
			var genericArguments = type.GetGenericArguments();
			var genericTypeDefinition = type.GetGenericTypeDefinition();
			var lastIndexOfTag = genericTypeDefinition.FullName.LastIndexOf('`');

			Out(genericTypeDefinition.FullName.Substring(0, lastIndexOfTag));
			Out("<");
			bool first = true;
			foreach (var genericArgument in genericArguments)
			{
				if (first == false)
					Out(", ");
				first = false;
				VisitType(genericArgument);
			}
			Out(">");
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.NewArrayExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitNewArray(NewArrayExpression node)
		{
			switch (node.NodeType)
			{
				case ExpressionType.NewArrayInit:
					Out("new ");
					if (!CheckIfAnonymousType(node.Type.GetElementType()))
					{
						Out(node.Type.GetElementType().FullName + " ");
					}
					Out("[]");
					VisitExpressions('{', node.Expressions, '}');
					return node;

				case ExpressionType.NewArrayBounds:
					Out("new " + node.Type);
					VisitExpressions('(', node.Expressions, ')');
					return node;
			}
			return node;
		}

		private static bool CheckIfAnonymousType(Type type)
		{
			// hack: the only way to detect anonymous types right now
			return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
				&& type.IsGenericType && type.Name.Contains("AnonymousType")
				&& (type.Name.StartsWith("<>") || type.Name.StartsWith("VB$"))
				&& (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
		}

		private static readonly HashSet<string> keywordsInCSharp = new HashSet<string>(new[]
		{
			"abstract",
			"as",
			"base",
			"bool",
			"break",
			"byte",
			"case",
			"catch",
			"char",
			"checked",
			"class",
			"const",
			"continue",
			"decimal",
			"default",
			"delegate",
			"do",
			"double",
			"else",
			"enum",
			"event",
			"explicit",
			"extern",
			"false",
			"finally",
			"fixed",
			"float",
			"for",
			"foreach",
			"goto",
			"if",
			"implicit",
			"in",
			"in (generic modifier)",
			"int",
			"interface",
			"internal",
			"is",
			"lock",
			"long",
			"namespace",
			"new",
			"null",
			"object",
			"operator",
			"out",
			"out (generic modifier)",
			"override",
			"params",
			"private",
			"protected",
			"public",
			"readonly",
			"ref",
			"return",
			"sbyte",
			"sealed",
			"short",
			"sizeof",
			"stackalloc",
			"static",
			"string",
			"struct",
			"switch",
			"this",
			"throw",
			"true",
			"try",
			"typeof",
			"uint",
			"ulong",
			"unchecked",
			"unsafe",
			"ushort",
			"using",
			"virtual",
			"void",
			"volatile",
			"while"
		});

		/// <summary>
		///   Visits the <see cref = "T:System.Linq.Expressions.ParameterExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitParameter(ParameterExpression node)
		{
			if (node.IsByRef)
			{
				Out("ref ");
			}
			if (string.IsNullOrEmpty(node.Name))
			{
				Out("Param_" + GetParamId(node));
				return node;
			}
			var name = node.Name.StartsWith("$VB$") ? node.Name.Substring(4) : node.Name;
			if (keywordsInCSharp.Contains(name))
				Out('@');
			Out(name);
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.RuntimeVariablesExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitRuntimeVariables(RuntimeVariablesExpression node)
		{
			VisitExpressions('(', node.Variables, ')');
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.SwitchExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitSwitch(SwitchExpression node)
		{
			Out("switch ");
			Out("(");
			Visit(node.SwitchValue);
			Out(") { ... }");
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.SwitchCase" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override SwitchCase VisitSwitchCase(SwitchCase node)
		{
			Out("case ");
			VisitExpressions('(', node.TestValues, ')');
			Out(": ...");
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.TryExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitTry(TryExpression node)
		{
			Out("try { ... }");
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.TypeBinaryExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitTypeBinary(TypeBinaryExpression node)
		{
			const ExpressionOperatorPrecedence currentPrecedence = ExpressionOperatorPrecedence.RelationalAndTypeTesting;
			string op;
			switch (node.NodeType)
			{
				case ExpressionType.TypeIs:
					op = " is ";
					break;

				case ExpressionType.TypeEqual:
					op = " TypeEqual ";
					break;
				default:
					throw new InvalidOperationException();
			}


			Visit(node.Expression, currentPrecedence);
			Out(op);
			Out(node.TypeOperand.Name);
			return node;
		}

		/// <summary>
		///   Visits the children of the <see cref = "T:System.Linq.Expressions.UnaryExpression" />.
		/// </summary>
		/// <param name = "node">The expression to visit.</param>
		/// <returns>
		///   The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitUnary(UnaryExpression node)
		{
			return VisitUnary(node, _currentPrecedence);
		}

		private Expression VisitUnary(UnaryExpression node, ExpressionOperatorPrecedence outerPrecedence)
		{
			var innerPrecedence = ExpressionOperatorPrecedence.Unary;

			switch (node.NodeType)
			{
				case ExpressionType.TypeAs:
					innerPrecedence = ExpressionOperatorPrecedence.RelationalAndTypeTesting;
					break;

				case ExpressionType.Decrement:
					innerPrecedence = ExpressionOperatorPrecedence.ParenthesisNotNeeded;
					Out("Decrement(");
					break;

				case ExpressionType.Negate:
				case ExpressionType.NegateChecked:
					Out("-");
					break;

				case ExpressionType.UnaryPlus:
					Out("+");
					break;

				case ExpressionType.Not:
					Out("!");
					break;

				case ExpressionType.Quote:
					break;

				case ExpressionType.Increment:
					innerPrecedence = ExpressionOperatorPrecedence.ParenthesisNotNeeded;
					Out("Increment(");
					break;

				case ExpressionType.Throw:
					innerPrecedence = ExpressionOperatorPrecedence.ParenthesisNotNeeded;
					Out("throw ");
					break;

				case ExpressionType.PreIncrementAssign:
					Out("++");
					break;

				case ExpressionType.PreDecrementAssign:
					Out("--");
					break;

				case ExpressionType.OnesComplement:
					Out("~");
					break;
				case ExpressionType.Convert:
				case ExpressionType.ConvertChecked:
					// we only cast enums and types is mscorlib), we don't support anything else
					// because the VB compiler like to put converts all over the place, and include
					// types that we can't really support (only exists on the client)
					var nonNullableType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
					if ((nonNullableType.IsEnum ||
						 nonNullableType.Assembly == typeof(string).Assembly) &&
						 (nonNullableType.IsGenericType == false))
					{
						Out("(");
						Out("(");
						Out(nonNullableType.FullName);
						if (Nullable.GetUnderlyingType(node.Type) != null)
							Out("?");
						Out(")");
					}
					Out("(");
					break;
				case ExpressionType.ArrayLength:
					// we don't want to do nothing for those
					Out("(");
					break;
				default:
					innerPrecedence = ExpressionOperatorPrecedence.ParenthesisNotNeeded;
					Out(node.NodeType.ToString());
					Out("(");
					break;
			}

			SometimesParenthesis(outerPrecedence, innerPrecedence, () => Visit(node.Operand, innerPrecedence));

			switch (node.NodeType)
			{
				case ExpressionType.TypeAs:
					Out(" As ");
					Out(node.Type.Name);
					break;

				case ExpressionType.ArrayLength:
					Out(".Length)");
					break;

				case ExpressionType.Decrement:
				case ExpressionType.Increment:
					Out(")");
					break;

				case ExpressionType.Convert:
				case ExpressionType.ConvertChecked:
					// we only cast enums and types is mscorlib), we don't support anything else
					// because the VB compiler like to put converts all over the place, and include
					// types that we can't really support (only exists on the client)
					var nonNullableType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
					if ((nonNullableType.IsEnum ||
						 nonNullableType.Assembly == typeof(string).Assembly) &&
						nonNullableType.IsGenericType == false)
					{
						Out(")");
					}
					Out(")");
					break;

				case ExpressionType.Negate:
				case ExpressionType.UnaryPlus:
				case ExpressionType.NegateChecked:
				case ExpressionType.Not:
				case ExpressionType.Quote:
				case ExpressionType.Throw:
				case ExpressionType.PreIncrementAssign:
				case ExpressionType.PreDecrementAssign:
				case ExpressionType.OnesComplement:
					break;

				case ExpressionType.PostIncrementAssign:
					Out("++");
					break;

				case ExpressionType.PostDecrementAssign:
					Out("--");
					break;

				default:
					Out(")");
					break;
			}

			return node;
		}
	}
}

#endif
