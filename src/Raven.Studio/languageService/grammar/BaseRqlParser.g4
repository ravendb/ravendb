parser grammar BaseRqlParser;


options { tokenVocab = BaseRqlLexer; }
prog
   : parameterBeforeQuery* functionStatment* fromStatement groupByStatement? whereStatement? orderByStatement? loadStatement? updateStatement? filterStatement? selectStatement? includeStatement? limitStatement? json? EOF
   ;
   //          FROM STATEMENT          //
   
functionStatment
   : jsFunction # javaScriptFunction
   | tsFunction # timeSeriesFunction
   ;

updateStatement
   : UPDATE US_OP updateBody* US_CL EOF
   ;

fromMode
   : FROM
   ;

fromStatement
   : fromMode INDEX collection = indexName aliasWithOptionalAs? # CollectionByIndex
   | FROM ALL_DOCS # AllCollections
   | fromMode collection = collectionName aliasWithOptionalAs? # CollectionByName
   ;

indexName
   : WORD
   | string
   | identifiersWithoutRootKeywords
   ;

collectionName
   : WORD
   | string
   | identifiersWithoutRootKeywords
   ;
   //We can use aliases like:
   
   //from X as t
   
   //from X t
   
aliasWithOptionalAs
   : AS? name = aliasName asArray?
   ;
   //          GROUP BY STATEMENT          //    
   
groupByMode
   : GROUP_BY
   ;

groupByStatement
   : groupByMode (value = parameterWithOptionalAlias) (COMMA (parameterWithOptionalAlias))*
   ;

suggestGroupBy
   :
   ;
   //          WHERE STATEMENT         //
   
whereMode
   : WHERE
   ;

whereStatement
   : whereMode expr
   ;

expr
   : left = expr binary right = expr # binaryExpression
   | OP_PAR expr CL_PAR # opPar
   | left = exprValue EQUAL right = exprValue # equalExpression
   | left = exprValue MATH right = exprValue # mathExpression
   | specialFunctions # specialFunctionst
   | inFunction # inExpr
   | betweenFunction # betweenExpr
   | funcExpr=function # normalFunc
   | TRUE AND NOT? expr # booleanExpression
   ;

binary
   : AND NOT
   | OR NOT
   | AND
   | OR
   ;

exprValue
   : literal # parameterExpr
   ;

inFunction
   : value = literal ALL? IN OP_PAR first = literal (COMMA next = literal)* CL_PAR
   ;

betweenFunction
   : value = literal BETWEEN from = literal AND to = literal
   ;
   //Functions like morelikethis() or intersect()
   
specialFunctions
   : specialFunctionName OP_PAR (specialParam aliasWithRequiredAs? (COMMA specialParam aliasWithRequiredAs?)*)? CL_PAR
   ;

specialFunctionName
   : ID
   | FUZZY
   | SEARCH
   | FACET
   | BOOST
   | STARTS_WITH
   | ENDS_WITH
   | MORELIKETHIS
   | INTERSECT
   | EXACT
   ;

specialParam
   : OP_PAR specialParam CL_PAR
   | specialParam EQUAL specialParam
   | variable BETWEEN specialParam
   | specialParam AND specialParam
   | specialParam OR specialParam
   | specialParam MATH specialParam
   | inFunction
   | betweenFunction
   | specialFunctions
   | date
   | function
   | variable
   | identifiersAllNames
   | NUM
   ;
   //          LOAD STATEMENT          //
   
   //Accept:n
   
   // load x as X
   
   //also list of load eg (load x as y, y as p) etc 
   
loadMode
   : LOAD
   ;

loadStatement
   : loadMode item = loadDocumentByName (COMMA loadDocumentByName)*
   ;

loadDocumentByName
   : name = variable as = aliasWithOptionalAs
   ;
   //          ORDER BY            //
   
orderByMode
   : ORDER_BY
   ;

orderByStatement
   : orderByMode value = orderByItem (COMMA (orderByItem))*
   ;

orderByItem
   : value = literal order = orderBySorting? orderValueType = orderByOrder?
   ;
   //Order sorting option keyword.
   
orderBySorting
   : AS sortingMode = orderBySortingAs
   ;

orderBySortingAs
   : STRING_W
   | ALPHANUMERIC
   | LONG
   | DOUBLE
   ;

orderByOrder
   : SORTING
   ;
   //          SELECT STATEMENT            //
   
selectMode
   : SELECT
   ;

selectStatement
   :
   //  Select only individual fields e.g. "select Column1 (as x)?, Column2"
   selectMode DISTINCT STAR limitStatement? # getAllDistinct
   | selectMode DISTINCT? field = projectField (COMMA projectField)* limitStatement? # ProjectIndividualFields
   | JS_SELECT jsBody* JS_CL limitStatement? # javascriptCode
   ;

projectField
   : (literal | specialFunctions | tsProg) aliasWithRequiredAs?
   ;
   //JS header
   
   //Accept declare function(params...). We don't get any JS CODE on first channel so we need to check it like in example from SELECT statement
   
jsFunction
   : JS_FUNCTION_DECLARATION JFN_WORD JFN_OP_PAR JFN_WORD? (JFN_COMMA JFN_WORD)* JFN_CL_PAR JFN_OP_JS jsBody* JS_CL
   //definition declare func(X,...y)
   
   ;

jsBody
   : JS_OP jsBody* JS_CL
   ;

aliasWithRequiredAs
   : AS name = aliasName asArray?
   ;

aliasName
   : (WORD | identifiersWithoutRootKeywords | string)
   ;
   // @metadata.
   
   // array like Array[].Empty[]. etc
   
prealias
   : METADATA DOT
   | ((WORD | string) asArray? DOT)+
   ;

asArray
   : OP_Q CL_Q
   ;
   //          INCLUDE STATEMENT           //
   
includeMode
   : INCLUDE
   ;

includeStatement
   : includeMode (tsIncludeTimeseriesFunction | literal) (COMMA (literal | tsIncludeTimeseriesFunction))*
   ;
   //          LIMIT STATEMENT          //
   
limitStatement
   : LIMIT variable ((COMMA | OFFSET) variable)? (FILTER_LIMIT variable)?
   | FILTER_LIMIT variable
   ;
   //          UTILS SEGMENT           //
   
   //Capture variable name (also accept aliased names).
   
variable
   : name=memberName DOT member=variable
   | name=memberName
   ;

memberName
    :
    cacheParam | param
    ;
    

param
   : (NUM | WORD | date | string | ID OP_PAR CL_PAR | identifiersAllNames) asArray?
   ;

literal
   : DOL? (function | variable)
   ;

cacheParam
   : DOL WORD
   ;

parameterWithOptionalAlias
   : value = variableOrFunction as = aliasWithRequiredAs?
   ;

variableOrFunction
   : variable
   | function
   ;
   //Function definition. It accept function with aliases, params or param-free.
   
function
   : addr=variable OP_PAR args=arguments? CL_PAR
   ;

arguments:
    literal (COMMA literal)*;
   
      
identifiersWithoutRootKeywords
   : ALL
   | AND
   | BETWEEN
   | DISTINCT
   | ENDS_WITH
   | STARTS_WITH
   | FALSE
   | FACET
   | IN
   | ID
   | INTERSECT
   | LONG
   | MATCH
   | MORELIKETHIS
   | NULL
   | OR
   | STRING_W
   | TRUE
   | WITH
   | EXACT
   | BOOST
   | SEARCH
   | FUZZY
   | METADATA
   | TO
   | NOT
   | SORTING
   | ALPHANUMERIC
   | DOUBLE
   ;

rootKeywords
   : FROM
   | GROUP_BY
   | WHERE
   | LOAD
   | ORDER_BY
   | SELECT
   | INCLUDE
   | LIMIT
   | INDEX
   | FILTER
   | FILTER_LIMIT
   ;

identifiersAllNames
   : identifiersWithoutRootKeywords
   | rootKeywords
   ;
   //Accept date range [DATE TO DATE]
   
date
   : OP_Q (NULL | dateString) TO (NULL | dateString) CL_Q
   ;

dateString
   : WORD DOT NUM
   ;
   //          TIMESERIES          //
   
tsProg
   : TIMESERIES tsQueryBody TS_CL_PAR
   ;

tsIncludeTimeseriesFunction
   : TIMESERIES tsLiteral (TS_COMMA tsIncludeLiteral)? (TS_COMMA tsIncludeLiteral)? TS_CL_PAR
   ;

tsIncludeLiteral
   : tsLiteral
   | tsIncludeSpecialMethod
   ;

tsIncludeSpecialMethod
   : (TS_LAST | TS_FIRST) TS_OP_PAR TS_NUM (TS_COMMA TS_STRING)? TS_CL_PAR
   ;

tsQueryBody
   : from = tsFROM range = tsTimeRangeStatement? load = tsLoadStatement? where = tsWHERE? groupBy = tsGroupBy? select = tsSelect? scale = tsSelectScaleProjection? offset = tsOffset?
   ;

tsOffset
   : TS_OFFSET TS_STRING
   ;

tsFunction
   : TIMESERIES_FUNCTION_DECLARATION TS_WORD TS_OP_PAR TS_WORD (TS_COMMA TS_WORD)* TS_CL_PAR TS_OP_C body = tsQueryBody TS_CL_C
   ;

tsTimeRangeStatement
   : tsBetween
   | first = tsTimeRangeFirst last = tsTimeRangeLast
   | first = tsTimeRangeFirst
   | last = tsTimeRangeLast
   ;

tsLoadStatement
   : TS_LOAD tsAlias
   ;

tsAlias
   : TS_AS alias_text = tsLiteral
   ;

tsFROM
   : TS_FROM name = tsCollectionName
   ;

tsWHERE
   : TS_WHERE tsExpr
   ;

tsExpr
   : left = tsExpr TS_MATH right = tsExpr # tsMathExpression
   | left = tsExpr tsBinary right = tsExpr # tsBinaryExpression
   | TS_OP_PAR tsExpr TS_CL_PAR # tsOpPar
   | TS_TRUE TS_AND TS_NOT? tsExpr # tsBooleanExpression
   | tsLiteral # tsLiteralExpression
   ;

tsBetween
   : TS_BETWEEN from = tsLiteral TS_AND to = tsLiteral
   ;

tsBinary
   : TS_AND
   | TS_OR
   ;

tsLiteral
   : TS_DOL (TS_WORD | TS_NUM | tsIdentifiers)
   | (TS_WORD | TS_STRING) (TS_OP_Q TS_NUM TS_CL_Q)? (TS_DOT (TS_WORD | TS_STRING | tsIdentifiers))*
   ;

tsTimeRangeFirst
   : TS_FIRST num = TS_NUM size = TS_TIMERANGE
   ;

tsTimeRangeLast
   : TS_LAST num = TS_NUM size = TS_TIMERANGE
   ;

tsCollectionName
   : (TS_WORD | TS_STRING | tsIdentifiers) (TS_DOT tsCollectionName)?
   ;

tsGroupBy
   : TS_GROUPBY name = tsCollectionName (TS_COMMA tsCollectionName)* TS_WITH?
   ;

tsSelect
   : TS_SELECT field = tsSelectVariable (TS_COMMA tsSelectVariable)*
   ;

tsSelectScaleProjection
   : TS_SCALE TS_NUM
   ;

tsSelectVariable
   : TS_METHOD
   | tsLiteral
   ;

tsIdentifiers
   : TS_OR
   | TS_AND
   | TS_FROM
   | TS_WHERE
   | TS_GROUPBY
   | TS_TIMERANGE
   ;

updateBody:
   US_OP updateBody* US_CL 
    ;

filterStatement:
   filterMode filterExpr;
   
filterExpr
  : left = filterExpr binary right = filterExpr # filterBinaryExpression
  | OP_PAR filterExpr CL_PAR # filterOpPar
  | left = exprValue EQUAL right = exprValue # filterEqualExpression
  | left = exprValue MATH right = exprValue # filterMathExpression
  | funcExpr=function # filterNormalFunc
  | TRUE AND NOT? expr # filterBooleanExpression
  ;
    
filterMode:
   FILTER;

parameterBeforeQuery:
   name=literal EQUAL value=parameterValue;

parameterValue
   : string
   | NUM;

json:
   jsonObj;

//This part is based on https://github.com/antlr/grammars-v4/blob/master/json/JSON.g4
//json
jsonObj
   : OP_CUR jsonPair (COMMA jsonPair)* CL_CUR
   | OP_CUR CL_CUR
   ;

jsonPair
   : DOUBLE_QUOTE_STRING ':' jsonValue
   ;

jsonArr
   : OP_Q jsonValue (COMMA jsonValue)* CL_Q
   | OP_Q CL_Q
   ;

jsonValue
   : DOUBLE_QUOTE_STRING
   | NUM
   | jsonObj
   | jsonArr
   | TRUE
   | FALSE
   | NULL
   ;
// end
string
   : DOUBLE_QUOTE_STRING
   | SINGLE_QUOTE_STRING
   ;
