namespace Lambda2Js
{
    public enum JavascriptOperationTypes
    {
        NoOp = -1,

        /// <summary>
        /// Parameters, all LHS operands in assignments, and other isolated value:
        /// <para>f(op)</para>
        /// <para>f(op, op)</para>
        /// <para>new f(op)</para>
        /// <para>new f(op, op)</para>
        /// <para>obj[op]</para>
        /// <para>[op]</para>
        /// <para>[op, op]</para>
        /// <para>{ Name: op }</para>
        /// <para>{ Name: op, Name: op }</para>
        /// <para>if (op) ...</para>
        /// <para>for (op; op; op) ...</para>
        /// <para>for (var i in op) ...</para>
        /// <para>op;</para>
        /// <para>return op;</para>
        /// <para>var x = op;</para>
        /// <para>var x = op, y = op;</para>
        /// <para>condition ? op : op</para>
        /// <para>op = value</para>
        /// <para>op += value</para>
        /// <para>op -= value</para>
        /// <para>op *= value</para>
        /// <para>op /= value</para>
        /// <para>op %= value</para>
        /// <para>op |= value</para>
        /// <para>op &amp;= value</para>
        /// <para>op ^= value</para>
        /// <para>() => value</para>
        /// </summary>
        ParamIsolatedLhs,

        /// <summary>
        /// Right hand side of an assignment.
        /// <para>ref = op</para>
        /// <para>ref += op</para>
        /// <para>ref -= op</para>
        /// <para>ref *= op</para>
        /// <para>ref /= op</para>
        /// <para>ref %= op</para>
        /// <para>ref |= op</para>
        /// <para>ref &amp;= op</para>
        /// <para>ref ^= op</para>
        /// </summary>
        AssignRhs,

        /// <summary>
        /// Ternary condition.
        /// <para>op ? true : false</para>
        /// </summary>
        TernaryCondition,

        /// <summary>
        /// Or and logic Or.
        /// <para>op | op</para>
        /// <para>op || op</para>
        /// </summary>
        Or,

        /// <summary>
        /// And, Exclusive-Or and logic And.
        /// <para>op &amp; op</para>
        /// <para>op ^ op</para>
        /// <para>op &amp;&amp; op</para>
        /// </summary>
        AndXor,

        /// <summary>
        /// Equals, not equals, greater, lower, not greater, not lower.
        /// <para>op == op</para>
        /// <para>op != op</para>
        /// <para>op > op</para>
        /// <para>op &lt; op</para>
        /// <para>op &lt;= op</para>
        /// <para>op >= op</para>
        /// </summary>
        Comparison,

        /// <summary>
        /// Add and subtract.
        /// <para>op + op</para>
        /// <para>op - op</para>
        /// </summary>
        AddSubtract,

        /// <summary>
        /// String concatenation.
        /// <para>op + op</para>
        /// </summary>
        Concat,

        /// <summary>
        /// Multiply, divide and get module.
        /// <para>op * op</para>
        /// <para>op / op</para>
        /// <para>op % op</para>
        /// </summary>
        MulDivMod,

        /// <summary>
        /// Shift operations.
        /// <para>op &lt;&lt; op</para>
        /// <para>op >> op</para>
        /// </summary>
        Shift,

        /// <summary>
        /// Negative, one's complement, and plus sign.
        /// <para>-op</para>
        /// <para>~op</para>
        /// <para>+op</para>
        /// </summary>
        NegComplPlus,

        /// <summary>
        /// Inline function definition.
        /// <para>function(){ statements }</para>
        /// </summary>
        InlineFunc,

        /// <summary>
        /// Calling a function.
        /// <para>op(parameters)</para>
        /// </summary>
        Call,

        /// <summary>
        /// New operator.
        /// <para>new op(parameters)</para>
        /// </summary>
        New,

        /// <summary>
        /// Literal.
        /// <para>1234</para>
        /// <para>.1</para>
        /// <para>3.9</para>
        /// <para>40.</para>
        /// <para>"string"</para>
        /// <para>'string'</para>
        /// <para>/regex/</para>
        /// </summary>
        Literal,

        /// <summary>
        /// Getting an indexed value or property.
        /// <para>op[value]</para>
        /// <para>op.Name</para>
        /// </summary>
        IndexerProperty,
    }
}