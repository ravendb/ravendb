%namespace Raven.Database.Indexing
%partial
%parsertype LuceneQueryParser
%visibility internal
%tokentype Token

%union { 
			public string s; 
			public FieldLuceneASTNode fn;
			public ParenthesistLuceneASTNode pn;
			public PostfixModifiers pm;
			public LuceneASTNodeBase nb;
			public OperatorLuceneASTNode.Operator o;
			public RangeLuceneASTNode rn;
			public TermLuceneASTNode tn;
			public MethodLuceneASTNode mn;
			public List<TermLuceneASTNode> ltn;
			public LuceneASTNodeBase.PrefixOperator npo;
	   }

%start main

%token NOT OR AND INTERSECT PLUS MINUS EOF OPEN_CURLY_BRACKET CLOSE_CURLY_BRACKET OPEN_SQUARE_BRACKET CLOSE_SQUARE_BRACKET 
%token TILDA BOOST QUOTE TO COLON OPEN_PAREN CLOSE_PAREN ALL_DOC
%token <s> UNANALIZED_TERM METHOD UNQUOTED_TERM QUOTED_TERM QUOTED_WILDCARD_TERM FLOAT_NUMBER INT_NUMBER DOUBLE_NUMBER LONG_NUMBER DATETIME NULL PREFIX_TERM WILDCARD_TERM HEX_NUMBER

%type <s> prefix_operator methodName fieldname  fuzzy_modifier boost_modifier proximity_modifier
%type <o> operator
%type <tn> term_exp term
%type <pm> postfix_modifier
%type <pn> paren_exp
%type <nb> main node
%type <fn> field_exp 
%type <rn> range_operator_exp
%type <mn> method_exp
%type <ltn> term_match_list
%type <npo> prefix_operator

%{
	public LuceneASTNodeBase LuceneAST {get; set;}
%}

%%

main: node EOF {
	//Console.WriteLine("Found rule main -> node EOF");
	$$ = $1;
	LuceneAST = $$;
	}
;
node: node operator node {
		//Console.WriteLine("Found rule node -> node operator node");
		var res =  new OperatorLuceneASTNode($1,$3,$2, IsDefaultOperatorAnd);
		$$ = res;
	}
	| node node          {
		//Console.WriteLine("Found rule node -> node node");
		$$ = new OperatorLuceneASTNode($1,$2,OperatorLuceneASTNode.Operator.Implicit, IsDefaultOperatorAnd);
	}
	| field_exp{
		//Console.WriteLine("Found rule node -> field_exp");
		$$ =$1;
	}
	| paren_exp{
		//Console.WriteLine("Found rule node -> paren_exp");
		$$ =$1;
	}
	| term_exp{
	//Console.WriteLine("Found rule node -> term_exp");
		$$ = $1;
	}
	| method_exp{
		//Console.WriteLine("Found rule node -> method_exp");
		$$ = $1;
	}
	| prefix_operator field_exp{
		//Console.WriteLine("Found rule node -> prefix_operator field_exp");
		$$ =$2;
		$$.Prefix = $1;
	}
	| prefix_operator paren_exp{
		//Console.WriteLine("Found rule node -> prefix_operator paren_exp");
		$$ =$2;
		$$.Prefix = $1;
	}
	| prefix_operator term_exp{
	//Console.WriteLine("Found rule node -> prefix_operator term_exp");
		$$ = $2;
		$$.Prefix = $1;
	}
	| prefix_operator method_exp{
		//Console.WriteLine("Found rule node -> prefix_operator method_exp");
		$$ = $2;
		$$.Prefix = $1;
	}
	| prefix_operator ALL_DOC
	{
		//Console.WriteLine("Found rule node -> prefix_operator ALL_DOC");
		$$ = new AllDocumentsLuceneASTNode();
		$$.Prefix = $1;
	}
	| ALL_DOC
	{
		$$ = new AllDocumentsLuceneASTNode();
	}
	;
field_exp: fieldname range_operator_exp {
		//Console.WriteLine("Found rule field_exp -> fieldname range_operator_exp");		
		$$ = new FieldLuceneASTNode(){FieldName = $1, Node = $2};
		}
	| fieldname term_exp                    {
		//Console.WriteLine("Found rule field_exp -> fieldname term_exp");
		$$ = new FieldLuceneASTNode(){FieldName = $1, Node = $2};
		}
	| fieldname paren_exp
	{
		//Console.WriteLine("Found rule field_exp -> fieldname paren_exp");
		$$ = new FieldLuceneASTNode(){FieldName = $1, Node = $2};
	}
	;

method_exp: methodName OPEN_PAREN term_match_list CLOSE_PAREN{
		//Console.WriteLine("Found rule method_exp -> methodName OPEN_PAREN term_match_list CLOSE_PAREN");
		$$ = new MethodLuceneASTNode($1,$3);
		InMethod = false;
}
| methodName OPEN_PAREN term_exp CLOSE_PAREN
{
		//Console.WriteLine("Found rule method_exp -> methodName OPEN_PAREN term_exp CLOSE_PAREN");
		$$ = new MethodLuceneASTNode($1,$3);
		InMethod = false;
}
;

term_match_list: term_exp term_exp 
{
	//Console.WriteLine("Found rule term_match_list -> term_exp term_exp");
	$$ = new List<TermLuceneASTNode>(){$1,$2};
}
| term_exp term_match_list 
{
	//Console.WriteLine("Found rule term_match_list -> term_exp term_match_list");
	$2.Add($1);
	$$ = $2;
}
;

paren_exp: OPEN_PAREN node CLOSE_PAREN {
		//Console.WriteLine("Found rule paren_exp -> OPEN_PAREN node CLOSE_PAREN");
		$$ = new ParenthesistLuceneASTNode();
		$$.Node = $2;
		}
	|  OPEN_PAREN node CLOSE_PAREN boost_modifier {
		//Console.WriteLine("Found rule paren_exp -> OPEN_PAREN node CLOSE_PAREN boost_modifier");
		$$ = new ParenthesistLuceneASTNode();
		$$.Node = $2;
		$$.Boost = $4;
		}
	; 
methodName: METHOD COLON{
		//Console.WriteLine("Found rule methodName -> METHOD COLON");
		$$ = $1;
		InMethod = true;
}
;
fieldname: UNQUOTED_TERM COLON {
		//Console.WriteLine("Found rule fieldname -> UNQUOTED_TERM COLON");
		$$ = $1;
	}
	;
term_exp: prefix_operator term postfix_modifier  {
		//Console.WriteLine("Found rule term_exp -> prefix_operator term postfix_modifier");
		$$ = $2;
		$$.Prefix =$1;
		$$.SetPostfixOperators($3);
	}
	| term postfix_modifier                      {
		//Console.WriteLine("Found rule term_exp -> postfix_modifier");
		$$ = $1;
		$$.SetPostfixOperators($2);
	}
	| prefix_operator term                       {
		//Console.WriteLine("Found rule term_exp -> prefix_operator term");
		$$ = $2;
		$$.Prefix = $1;
	}
	| term                                       {
		//Console.WriteLine("Found rule term_exp -> term");
		$$ = $1;
	}
	;
term: QUOTED_TERM        {
		//Console.WriteLine("Found rule term -> QUOTED_TERM");
		$$ = new TermLuceneASTNode(){Term=$1.Substring(1,$1.Length-2), Type=TermLuceneASTNode.TermType.Quoted};
	}
	| UNQUOTED_TERM      {
		//Console.WriteLine("Found rule term -> UNQUOTED_TERM");
		$$ = new TermLuceneASTNode(){Term=$1,Type=TermLuceneASTNode.TermType.UnQuoted};
		}
	|	INT_NUMBER {
		//Console.WriteLine("Found rule term -> INT_NUMBER");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.Int};
		}
	|	FLOAT_NUMBER {
		//Console.WriteLine("Found rule term -> FLOAT_NUMBER");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.Float};
	}
	| HEX_NUMBER {
		//Console.WriteLine("Found rule term -> HEX_NUMBER");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.Hex};
	}
	|	LONG_NUMBER {
		//Console.WriteLine("Found rule term -> INT_NUMBER");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.Long};
		}
	|	DOUBLE_NUMBER {
		//Console.WriteLine("Found rule term -> FLOAT_NUMBER");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.Double};
	}
	| UNANALIZED_TERM{
		//Console.WriteLine("Found rule term -> UNANALIZED_TERM");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.UnAnalyzed};
	}
	| DATETIME{
		//Console.WriteLine("Found rule term -> DATETIME");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.DateTime};
	}	
	| NULL {
		//Console.WriteLine("Found rule term -> NULL");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.Null};
	}

	| QUOTED_WILDCARD_TERM
	{
		//Console.WriteLine("Found rule term -> QUOTED_WILDCARD_TERM");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.QuotedWildcard};
	}

	| WILDCARD_TERM
	{
		//Console.WriteLine("Found rule term -> WILDCARD_TERM");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.WildCardTerm};
	}

	| PREFIX_TERM
	{
		//Console.WriteLine("Found rule term -> PREFIX_TERM");
		$$ = new TermLuceneASTNode(){Term=$1, Type=TermLuceneASTNode.TermType.PrefixTerm};
	}
	;
postfix_modifier: proximity_modifier boost_modifier 
	{
		$$ = new PostfixModifiers(){Boost = $2, Similerity = null, Proximity = $1};
	}
	| fuzzy_modifier boost_modifier 
	{
		$$ = new PostfixModifiers(){Boost = $2, Similerity = $1, Proximity = null};
	}
	| boost_modifier
	{
		$$ = new PostfixModifiers(){Boost = $1,Similerity = null, Proximity = null};
	}
	| fuzzy_modifier
	{
		$$ = new PostfixModifiers(){Boost = null, Similerity = $1, Proximity = null};
	}
	| proximity_modifier
	{
		$$ = new PostfixModifiers(){Boost = null, Similerity = null, Proximity = $1};
	}
	;
proximity_modifier: TILDA INT_NUMBER {
	//Console.WriteLine("Found rule proximity_modifier -> TILDA INT_NUMBER");
	$$ = $2;
	}	
;
boost_modifier: BOOST INT_NUMBER {
	//Console.WriteLine("Found rule boost_modifier -> BOOST INT_NUMBER");
	$$ = $2;
	}
	| BOOST FLOAT_NUMBER {
	//Console.WriteLine("Found rule boost_modifier -> BOOST FLOAT_NUMBER");
	$$ = $2;
	}
;
fuzzy_modifier: TILDA FLOAT_NUMBER {
	//Console.WriteLine("Found rule fuzzy_modifier ->  TILDA FLOAT_NUMBER");
	$$ = $2;
	}
	|TILDA {
		//Console.WriteLine("Found rule fuzzy_modifier ->  TILDA");
		$$ = "0.5";
	}
;

range_operator_exp: OPEN_CURLY_BRACKET term TO term CLOSE_CURLY_BRACKET {
		//Console.WriteLine("Found rule range_operator_exp -> OPEN_CURLY_BRACKET term TO term CLOSE_CURLY_BRACKET");
		$$ = new RangeLuceneASTNode(){RangeMin = $2, RangeMax = $4, InclusiveMin = false, InclusiveMax = false};
		}
	| OPEN_SQUARE_BRACKET term TO term CLOSE_CURLY_BRACKET  {
		//Console.WriteLine("Found rule range_operator_exp -> OPEN_SQUARE_BRACKET term TO term CLOSE_CURLY_BRACKET");
		$$ = new RangeLuceneASTNode(){RangeMin = $2, RangeMax = $4, InclusiveMin = true, InclusiveMax = false};
		}
	| OPEN_CURLY_BRACKET term TO term CLOSE_SQUARE_BRACKET  {
		//Console.WriteLine("Found rule range_operator_exp -> OPEN_CURLY_BRACKET term TO term CLOSE_SQUARE_BRACKET");
		$$ = new RangeLuceneASTNode(){RangeMin = $2, RangeMax = $4, InclusiveMin = false, InclusiveMax = true};
		}
	| OPEN_SQUARE_BRACKET term TO term CLOSE_SQUARE_BRACKET {
		//Console.WriteLine("Found rule range_operator_exp -> OPEN_SQUARE_BRACKET term TO term CLOSE_SQUARE_BRACKET");
		$$ = new RangeLuceneASTNode(){RangeMin = $2, RangeMax = $4, InclusiveMin = true, InclusiveMax = true};
		}
	;
operator: OR {
		//Console.WriteLine("Found rule operator -> OR");
		$$ = OperatorLuceneASTNode.Operator.OR;
		}
	| AND {
		//Console.WriteLine("Found rule operator -> AND");
		$$ = OperatorLuceneASTNode.Operator.AND;
		}
	| INTERSECT {
		//Console.WriteLine("Found rule operator -> INTERSECT");
		$$ = OperatorLuceneASTNode.Operator.INTERSECT;
	}
	;
prefix_operator: PLUS {
		//Console.WriteLine("Found rule prefix_operator -> PLUS");
		$$ = LuceneASTNodeBase.PrefixOperator.Plus;
		}
	| MINUS {
		//Console.WriteLine("Found rule prefix_operator -> MINUS");
		$$ = LuceneASTNodeBase.PrefixOperator.Minus;
		}
    | NOT {
        //Console.WriteLine("Found rule prefix_operator -> NOT");
		$$ = LuceneASTNodeBase.PrefixOperator.Minus;
    }
	;
%%