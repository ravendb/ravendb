#if !NET_3_5
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Client.Document;


namespace Raven.Client.Indexes
{
    /// <summary>
    /// Based off of System.Linq.Expressions.ExpressionStringBuilder
    /// </summary>
    public class ExpressionStringBuilder : ExpressionVisitor
    {
        // Fields
        private Dictionary<object, int> _ids;
        private StringBuilder _out = new StringBuilder();
        ExpressionOperatorPrecedence _currentPrecedence;
    	private DocumentConvention convention;

    	// Methods
        private ExpressionStringBuilder(DocumentConvention convention)
        {
        	this.convention = convention;
        }

    	private void AddLabel(LabelTarget label)
        {
            if (this._ids == null)
            {
                this._ids = new Dictionary<object, int>();
                this._ids.Add(label, 0);
            } else if (!this._ids.ContainsKey(label))
            {
                this._ids.Add(label, this._ids.Count);
            }
        }

        private void AddParam(ParameterExpression p)
        {
            if (this._ids == null)
            {
                this._ids = new Dictionary<object, int>();
                this._ids.Add(this._ids, 0);
            } else if (!this._ids.ContainsKey(p))
            {
                this._ids.Add(p, this._ids.Count);
            }
        }

        internal string CatchBlockToString(CatchBlock node)
        {
            ExpressionStringBuilder builder = new ExpressionStringBuilder(convention);
            builder.VisitCatchBlock(node);
            return builder.ToString();
        }

        private void DumpLabel(LabelTarget target)
        {
            if (!string.IsNullOrEmpty(target.Name))
            {
                this.Out(target.Name);
            } else
            {
                this.Out("UnamedLabel_" + this.GetLabelId(target));
            }
        }

        internal string ElementInitBindingToString(ElementInit node)
        {
            ExpressionStringBuilder builder = new ExpressionStringBuilder(convention);
            builder.VisitElementInit(node);
            return builder.ToString();
        }

		/// <summary>
		/// Convert the expression to a string
		/// </summary>
		/// <param name="convention">The convention.</param>
		/// <param name="node">The node.</param>
        public static string ExpressionToString(DocumentConvention convention, Expression node)
        {
        	ExpressionStringBuilder builder = new ExpressionStringBuilder(convention);
            builder.Visit(node, ExpressionOperatorPrecedence.ParenthesisNotNeeded);
            return builder.ToString();
        }

        private static string FormatBinder(CallSiteBinder binder)
        {
            ConvertBinder binder2 = binder as ConvertBinder;
            if (binder2 != null)
            {
                return ("Convert " + binder2.Type);
            }
            GetMemberBinder binder3 = binder as GetMemberBinder;
            if (binder3 != null)
            {
                return ("GetMember " + binder3.Name);
            }
            SetMemberBinder binder4 = binder as SetMemberBinder;
            if (binder4 != null)
            {
                return ("SetMember " + binder4.Name);
            }
            DeleteMemberBinder binder5 = binder as DeleteMemberBinder;
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
            InvokeMemberBinder binder6 = binder as InvokeMemberBinder;
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
            UnaryOperationBinder binder7 = binder as UnaryOperationBinder;
            if (binder7 != null)
            {
                return binder7.Operation.ToString();
            }
            BinaryOperationBinder binder8 = binder as BinaryOperationBinder;
            if (binder8 != null)
            {
                return binder8.Operation.ToString();
            }
            return "CallSiteBinder";
        }

        private int GetLabelId(LabelTarget label)
        {
            int count;
            if (this._ids == null)
            {
                this._ids = new Dictionary<object, int>();
                this.AddLabel(label);
                return 0;
            }
            if (!this._ids.TryGetValue(label, out count))
            {
                count = this._ids.Count;
                this.AddLabel(label);
            }
            return count;
        }

        private int GetParamId(ParameterExpression p)
        {
            int count;
            if (this._ids == null)
            {
                this._ids = new Dictionary<object, int>();
                this.AddParam(p);
                return 0;
            }
            if (!this._ids.TryGetValue(p, out count))
            {
                count = this._ids.Count;
                this.AddParam(p);
            }
            return count;
        }

        internal string MemberBindingToString(MemberBinding node)
        {
            ExpressionStringBuilder builder = new ExpressionStringBuilder(convention);
            builder.VisitMemberBinding(node);
            return builder.ToString();
        }

        private void Out(char c)
        {
            this._out.Append(c);
        }

        private void Out(string s)
        {
            this._out.Append(s);
        }

        private void OutMember(Expression instance, MemberInfo member)
        {
        	var name = member.Name;
        	var identityProperty = convention.GetIdentityProperty(member.DeclaringType);
			if (identityProperty == member)
				name = "__document_id";
        	if (instance != null)
            {
                this.Visit(instance);
                this.Out("." + name);
            } 
			else
            {
                this.Out(member.DeclaringType.Name + "." + name);
            }
        }

    	internal string SwitchCaseToString(SwitchCase node)
        {
            ExpressionStringBuilder builder = new ExpressionStringBuilder(convention);
            builder.VisitSwitchCase(node);
            return builder.ToString();
        }

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="System.String"/> that represents this instance.
		/// </returns>
        public override string ToString()
        {
            return this._out.ToString();
        }

        private void SometimesParenthesis(ExpressionOperatorPrecedence outer, ExpressionOperatorPrecedence inner, Action visitor)
        {
            bool needParenthesis = outer.NeedsParenthesisFor(inner);

            if (needParenthesis)
                this.Out("(");

            visitor();

            if (needParenthesis)
                this.Out(")");
        }

        private void Visit(Expression node, ExpressionOperatorPrecedence outerPrecedence)
        {
            ExpressionOperatorPrecedence previous = _currentPrecedence;
            _currentPrecedence = outerPrecedence;
            Visit(node);
            _currentPrecedence = previous;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.BinaryExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
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
                } else
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
                } else
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

                SometimesParenthesis(outerPrecedence, innerPrecedence, delegate()
                    {
                        this.Visit(leftOp, innerPrecedence);
                        this.Out("[");
                        this.Visit(rightOp, innerPrecedence);
                        this.Out("]");
                    });
                return node;

            default:
                throw new InvalidOperationException();
            }


            SometimesParenthesis(outerPrecedence, innerPrecedence, delegate()
                {
                    this.Visit(leftOp, innerPrecedence);
                    this.Out(' ');
                    this.Out(str);
                    this.Out(' ');
                    this.Visit(rightOp, innerPrecedence);
                });

            return node;
        }

    	private static void FixupEnumBinaryExpression(ref Expression left, ref Expression right)
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
					right = Expression.Constant(Enum.ToObject(expression.Type, constantExpression.Value).ToString());
    				break;
    		}
    	}

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.BlockExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
    	protected override Expression VisitBlock(BlockExpression node)
        {
            this.Out("{");
            foreach (ParameterExpression expression in node.Variables)
            {
                this.Out("var ");
                this.Visit(expression);
                this.Out(";");
            }
            this.Out(" ... }");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.CatchBlock"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override CatchBlock VisitCatchBlock(CatchBlock node)
        {
            this.Out("catch (" + node.Test.Name);
            if (node.Variable != null)
            {
                this.Out(node.Variable.Name ?? "");
            }
            this.Out(") { ... }");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.ConditionalExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitConditional(ConditionalExpression node)
        {
            return VisitConditional(node, _currentPrecedence);
        }

        private Expression VisitConditional(ConditionalExpression node, ExpressionOperatorPrecedence outerPrecedence)
        {
            ExpressionOperatorPrecedence innerPrecedence = ExpressionOperatorPrecedence.Conditional;

            SometimesParenthesis(outerPrecedence, innerPrecedence, delegate() {
                this.Visit(node.Test, innerPrecedence);
                this.Out(" ? ");
                this.Visit(node.IfTrue, innerPrecedence);
                this.Out(" : ");
                this.Visit(node.IfFalse, innerPrecedence);
            });

            return node;
        }

		/// <summary>
		/// Visits the <see cref="T:System.Linq.Expressions.ConstantExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value != null)
            {
                string s = node.Value.ToString();
                if (node.Value is string)
                {
                    this.Out("\"");
                    this.Out(s);
                    this.Out("\"");
                    return node;
                }
                if (s == node.Value.GetType().ToString())
                {
                    this.Out("value(");
                    this.Out(s);
                    this.Out(")");
                    return node;
                }
                this.Out(s);
                return node;
            }
            this.Out("null");
            return node;
        }

		/// <summary>
		/// Visits the <see cref="T:System.Linq.Expressions.DebugInfoExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitDebugInfo(DebugInfoExpression node)
        {
            string s = string.Format(CultureInfo.CurrentCulture, "<DebugInfo({0}: {1}, {2}, {3}, {4})>", new object[] { node.Document.FileName, node.StartLine, node.StartColumn, node.EndLine, node.EndColumn });
            this.Out(s);
            return node;
        }

		/// <summary>
		/// Visits the <see cref="T:System.Linq.Expressions.DefaultExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitDefault(DefaultExpression node)
        {
            this.Out("default(");
            this.Out(node.Type.Name);
            this.Out(")");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.DynamicExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitDynamic(DynamicExpression node)
        {
            this.Out(FormatBinder(node.Binder));
            this.VisitExpressions<Expression>('(', node.Arguments, ')');
            return node;
        }

		/// <summary>
		/// Visits the element init.
		/// </summary>
		/// <param name="initializer">The initializer.</param>
		/// <returns></returns>
        protected override ElementInit VisitElementInit(ElementInit initializer)
        {
            this.Out(initializer.AddMethod.ToString());
            this.VisitExpressions<Expression>('(', initializer.Arguments, ')');
            return initializer;
        }

        private void VisitExpressions<T>(char open, IList<T> expressions, char close) where T : Expression
        {
            this.Out(open);
            if (expressions != null)
            {
                bool flag = true;
                foreach (T local in expressions)
                {
                    if (flag)
                    {
                        flag = false;
                    } else
                    {
                        this.Out(", ");
                    }
                    this.Visit(local, ExpressionOperatorPrecedence.ParenthesisNotNeeded);
                }
            }
            this.Out(close);
        }

		/// <summary>
		/// Visits the children of the extension expression.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitExtension(Expression node)
        {
            BindingFlags bindingAttr = BindingFlags.ExactBinding | BindingFlags.Public | BindingFlags.Instance;
            if (node.GetType().GetMethod("ToString", bindingAttr, null, Type.EmptyTypes, null).DeclaringType != typeof(Expression))
            {
                this.Out(node.ToString());
                return node;
            }
            this.Out("[");
            if (node.NodeType == ExpressionType.Extension)
            {
                this.Out(node.GetType().FullName);
            } else
            {
                this.Out(node.NodeType.ToString());
            }
            this.Out("]");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.GotoExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitGoto(GotoExpression node)
        {
            this.Out(node.Kind.ToString().ToLower(CultureInfo.CurrentCulture));
            this.DumpLabel(node.Target);
            if (node.Value != null)
            {
                this.Out(" (");
                this.Visit(node.Value);
                this.Out(") ");
            }
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.IndexExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitIndex(IndexExpression node)
        {
            if (node.Object != null)
            {
                this.Visit(node.Object);
            } else
            {
                this.Out(node.Indexer.DeclaringType.Name);
            }
            if (node.Indexer != null)
            {
                this.Out(".");
                this.Out(node.Indexer.Name);
            }
            this.VisitExpressions<Expression>('[', node.Arguments, ']');
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.InvocationExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitInvocation(InvocationExpression node)
        {
            this.Out("Invoke(");
            this.Visit(node.Expression);
            int num = 0;
            int count = node.Arguments.Count;
            while (num < count)
            {
                this.Out(", ");
                this.Visit(node.Arguments[num]);
                num++;
            }
            this.Out(")");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.LabelExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitLabel(LabelExpression node)
        {
            this.Out("{ ... } ");
            this.DumpLabel(node.Target);
            this.Out(":");
            return node;
        }

		/// <summary>
		/// Visits the lambda.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="node">The node.</param>
		/// <returns></returns>
        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            if (node.Parameters.Count == 1)
            {
                this.Visit(node.Parameters[0]);
            } else
            {
                this.VisitExpressions<ParameterExpression>('(', node.Parameters, ')');
            }
            this.Out(" => ");
            this.Visit(node.Body);
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.ListInitExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitListInit(ListInitExpression node)
        {
            this.Visit(node.NewExpression);
            this.Out(" {");
            int num = 0;
            int count = node.Initializers.Count;
            while (num < count)
            {
                if (num > 0)
                {
                    this.Out(", ");
                }
                this.Out(node.Initializers[num].ToString());
                num++;
            }
            this.Out("}");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.LoopExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitLoop(LoopExpression node)
        {
            this.Out("loop { ... }");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.MemberExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitMember(MemberExpression node)
        {
            this.OutMember(node.Expression, node.Member);
            return node;
        }

		/// <summary>
		/// Visits the member assignment.
		/// </summary>
		/// <param name="assignment">The assignment.</param>
		/// <returns></returns>
        protected override MemberAssignment VisitMemberAssignment(MemberAssignment assignment)
        {
            this.Out(assignment.Member.Name);
            this.Out(" = ");
            this.Visit(assignment.Expression);
            return assignment;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.MemberInitExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            if ((node.NewExpression.Arguments.Count == 0) && node.NewExpression.Type.Name.Contains("<"))
            {
                this.Out("new");
            } else
            {
                this.Visit(node.NewExpression);
            }
            this.Out(" {");
            int num = 0;
            int count = node.Bindings.Count;
            while (num < count)
            {
                MemberBinding binding = node.Bindings[num];
                if (num > 0)
                {
                    this.Out(", ");
                }
                this.VisitMemberBinding(binding);
                num++;
            }
            this.Out("}");
            return node;
        }

		/// <summary>
		/// Visits the member list binding.
		/// </summary>
		/// <param name="binding">The binding.</param>
		/// <returns></returns>
        protected override MemberListBinding VisitMemberListBinding(MemberListBinding binding)
        {
            this.Out(binding.Member.Name);
            this.Out(" = {");
            int num = 0;
            int count = binding.Initializers.Count;
            while (num < count)
            {
                if (num > 0)
                {
                    this.Out(", ");
                }
                this.VisitElementInit(binding.Initializers[num]);
                num++;
            }
            this.Out("}");
            return binding;
        }

		/// <summary>
		/// Visits the member member binding.
		/// </summary>
		/// <param name="binding">The binding.</param>
		/// <returns></returns>
        protected override MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding binding)
        {
            this.Out(binding.Member.Name);
            this.Out(" = {");
            int num = 0;
            int count = binding.Bindings.Count;
            while (num < count)
            {
                if (num > 0)
                {
                    this.Out(", ");
                }
                this.VisitMemberBinding(binding.Bindings[num]);
                num++;
            }
            this.Out("}");
            return binding;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.MethodCallExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            int num = 0;
            Expression expression = node.Object;
            if (Attribute.GetCustomAttribute(node.Method, typeof(ExtensionAttribute)) != null)
            {
                num = 1;
                expression = node.Arguments[0];
            }
            if (expression != null)
            {
                if (expression.Type == typeof(IClientSideDatabase))
                {
                    this.Out("Database");
                }
                else
                {
                    this.Visit(expression);
                }
                if (node.Method.Name != "get_Item") // VB indexer
                {
                    this.Out(".");
                }
            }
            if(node.Method.Name != "get_Item") // VB indexer
            {
                this.Out(node.Method.Name);
                this.Out("(");
            }
            else
            {
                this.Out("[");
            }
		    int num2 = num;
            int count = node.Arguments.Count;
            while (num2 < count)
            {
                if (num2 > num)
                {
                    this.Out(", ");
                }
                this.Visit(node.Arguments[num2]);
                num2++;
            }
            if (node.Method.Name != "get_Item")// VB indexer
            {
                this.Out(")");
            }
            else
            {
                this.Out("]");
            }
		    return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.NewExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitNew(NewExpression node)
        {
            this.Out("new " + node.Type.Name);
            this.Out("(");
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                if (i > 0)
                {
                    this.Out(", ");
                }
                if (node.Members != null)
                {
                    this.Out(node.Members[i].Name);
                    this.Out(" = ");
                }
                this.Visit(node.Arguments[i]);
            }
            this.Out(")");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.NewArrayExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitNewArray(NewArrayExpression node)
        {
            switch (node.NodeType)
            {
            case ExpressionType.NewArrayInit:
                this.Out("new [] ");
                this.VisitExpressions<Expression>('{', node.Expressions, '}');
                return node;

            case ExpressionType.NewArrayBounds:
                this.Out("new " + node.Type.ToString());
                this.VisitExpressions<Expression>('(', node.Expressions, ')');
                return node;
            }
            return node;
        }

		/// <summary>
		/// Visits the <see cref="T:System.Linq.Expressions.ParameterExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node.IsByRef)
            {
                this.Out("ref ");
            }
            if (string.IsNullOrEmpty(node.Name))
            {
                this.Out("Param_" + this.GetParamId(node));
                return node;
            }
            if(node.Name.StartsWith("$VB$"))
            {
                this.Out(node.Name.Substring(4));
            }
            else
            {
                this.Out(node.Name);
            }
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.RuntimeVariablesExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitRuntimeVariables(RuntimeVariablesExpression node)
        {
            this.VisitExpressions<ParameterExpression>('(', node.Variables, ')');
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.SwitchExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitSwitch(SwitchExpression node)
        {
            this.Out("switch ");
            this.Out("(");
            this.Visit(node.SwitchValue);
            this.Out(") { ... }");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.SwitchCase"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override SwitchCase VisitSwitchCase(SwitchCase node)
        {
            this.Out("case ");
            this.VisitExpressions<Expression>('(', node.TestValues, ')');
            this.Out(": ...");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.TryExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitTry(TryExpression node)
        {
            this.Out("try { ... }");
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.TypeBinaryExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitTypeBinary(TypeBinaryExpression node)
        {
            return VisitTypeBinary(node, ExpressionOperatorPrecedence.ParenthesisNotNeeded);
        }

        private Expression VisitTypeBinary(TypeBinaryExpression node, ExpressionOperatorPrecedence outerPrecedence)
        {
            ExpressionOperatorPrecedence currentPrecedence = ExpressionOperatorPrecedence.RelationalAndTypeTesting;
            string op;
            switch (node.NodeType)
            {
            case ExpressionType.TypeIs:
                op = " is ";
                break;

            case ExpressionType.TypeEqual:
                op= " TypeEqual ";
                break;
            default:
                throw new InvalidOperationException();
            }


            this.Visit(node.Expression, currentPrecedence);
            this.Out(op);
            this.Out(node.TypeOperand.Name);
            return node;
        }

		/// <summary>
		/// Visits the children of the <see cref="T:System.Linq.Expressions.UnaryExpression"/>.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
        protected override Expression VisitUnary(UnaryExpression node)
        {
            return VisitUnary(node, _currentPrecedence);
        }

        private Expression VisitUnary(UnaryExpression node, ExpressionOperatorPrecedence outerPrecedence)
        {
            ExpressionOperatorPrecedence innerPrecedence = ExpressionOperatorPrecedence.Unary;

            switch (node.NodeType)
            {
            case ExpressionType.TypeAs:
                innerPrecedence = ExpressionOperatorPrecedence.RelationalAndTypeTesting;
                break;

            case ExpressionType.Decrement:
                innerPrecedence = ExpressionOperatorPrecedence.ParenthesisNotNeeded;
                this.Out("Decrement(");
                break;

            case ExpressionType.Negate:
            case ExpressionType.NegateChecked:
                this.Out("-");
                break;

            case ExpressionType.UnaryPlus:
                this.Out("+");
                break;

            case ExpressionType.Not:
                this.Out("!");
                break;

            case ExpressionType.Quote:
                break;

            case ExpressionType.Increment:
                innerPrecedence = ExpressionOperatorPrecedence.ParenthesisNotNeeded;
                this.Out("Increment(");
                break;

            case ExpressionType.Throw:
                innerPrecedence = ExpressionOperatorPrecedence.ParenthesisNotNeeded;
                this.Out("throw ");
                break;

            case ExpressionType.PreIncrementAssign:
                this.Out("++");
                break;

            case ExpressionType.PreDecrementAssign:
                this.Out("--");
                break;

            case ExpressionType.OnesComplement:
                this.Out("~");
                break;
            case ExpressionType.Convert:
            case ExpressionType.ConvertChecked:
                // we don't want to do nothing for those
                this.Out("(");
                break;
            default:
                innerPrecedence = ExpressionOperatorPrecedence.ParenthesisNotNeeded;
                this.Out(node.NodeType.ToString());
                this.Out("(");
                break;
            }

            SometimesParenthesis(outerPrecedence, innerPrecedence, delegate()
                {
                    this.Visit(node.Operand, innerPrecedence);
                });

            switch (node.NodeType)
            {
            case ExpressionType.TypeAs:
                this.Out(" As ");
                this.Out(node.Type.Name);
                break;

            case ExpressionType.Decrement:
            case ExpressionType.Increment:
                this.Out(")");
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
                this.Out("++");
                break;

            case ExpressionType.PostDecrementAssign:
                this.Out("--");
                break;

            default:
                this.Out(")");
                break;
            }

            return node;
        }
    }


}
#endif
