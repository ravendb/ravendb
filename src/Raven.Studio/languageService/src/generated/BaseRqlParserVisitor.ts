// Generated from languageService/grammar/BaseRqlParser.g4 by ANTLR 4.9.0-SNAPSHOT


import { ParseTreeVisitor } from "antlr4ts/tree/ParseTreeVisitor";

import { CollectionByIndexContext } from "./BaseRqlParser";
import { AllCollectionsContext } from "./BaseRqlParser";
import { CollectionByNameContext } from "./BaseRqlParser";
import { GetAllDistinctContext } from "./BaseRqlParser";
import { ProjectIndividualFieldsContext } from "./BaseRqlParser";
import { JavascriptCodeContext } from "./BaseRqlParser";
import { ParameterExprContext } from "./BaseRqlParser";
import { BinaryExpressionContext } from "./BaseRqlParser";
import { OpParContext } from "./BaseRqlParser";
import { EqualExpressionContext } from "./BaseRqlParser";
import { MathExpressionContext } from "./BaseRqlParser";
import { SpecialFunctionstContext } from "./BaseRqlParser";
import { InExprContext } from "./BaseRqlParser";
import { BetweenExprContext } from "./BaseRqlParser";
import { NormalFuncContext } from "./BaseRqlParser";
import { BooleanExpressionContext } from "./BaseRqlParser";
import { FilterBinaryExpressionContext } from "./BaseRqlParser";
import { FilterOpParContext } from "./BaseRqlParser";
import { FilterEqualExpressionContext } from "./BaseRqlParser";
import { FilterMathExpressionContext } from "./BaseRqlParser";
import { FilterNormalFuncContext } from "./BaseRqlParser";
import { FilterBooleanExpressionContext } from "./BaseRqlParser";
import { JavaScriptFunctionContext } from "./BaseRqlParser";
import { TimeSeriesFunctionContext } from "./BaseRqlParser";
import { TsMathExpressionContext } from "./BaseRqlParser";
import { TsBinaryExpressionContext } from "./BaseRqlParser";
import { TsOpParContext } from "./BaseRqlParser";
import { TsBooleanExpressionContext } from "./BaseRqlParser";
import { TsLiteralExpressionContext } from "./BaseRqlParser";
import { ProgContext } from "./BaseRqlParser";
import { FunctionStatmentContext } from "./BaseRqlParser";
import { UpdateStatementContext } from "./BaseRqlParser";
import { FromModeContext } from "./BaseRqlParser";
import { FromStatementContext } from "./BaseRqlParser";
import { IndexNameContext } from "./BaseRqlParser";
import { CollectionNameContext } from "./BaseRqlParser";
import { AliasWithOptionalAsContext } from "./BaseRqlParser";
import { GroupByModeContext } from "./BaseRqlParser";
import { GroupByStatementContext } from "./BaseRqlParser";
import { SuggestGroupByContext } from "./BaseRqlParser";
import { WhereModeContext } from "./BaseRqlParser";
import { WhereStatementContext } from "./BaseRqlParser";
import { ExprContext } from "./BaseRqlParser";
import { BinaryContext } from "./BaseRqlParser";
import { ExprValueContext } from "./BaseRqlParser";
import { InFunctionContext } from "./BaseRqlParser";
import { BetweenFunctionContext } from "./BaseRqlParser";
import { SpecialFunctionsContext } from "./BaseRqlParser";
import { SpecialFunctionNameContext } from "./BaseRqlParser";
import { SpecialParamContext } from "./BaseRqlParser";
import { LoadModeContext } from "./BaseRqlParser";
import { LoadStatementContext } from "./BaseRqlParser";
import { LoadDocumentByNameContext } from "./BaseRqlParser";
import { OrderByModeContext } from "./BaseRqlParser";
import { OrderByStatementContext } from "./BaseRqlParser";
import { OrderByItemContext } from "./BaseRqlParser";
import { OrderBySortingContext } from "./BaseRqlParser";
import { OrderBySortingAsContext } from "./BaseRqlParser";
import { OrderByOrderContext } from "./BaseRqlParser";
import { SelectModeContext } from "./BaseRqlParser";
import { SelectStatementContext } from "./BaseRqlParser";
import { ProjectFieldContext } from "./BaseRqlParser";
import { JsFunctionContext } from "./BaseRqlParser";
import { JsBodyContext } from "./BaseRqlParser";
import { AliasWithRequiredAsContext } from "./BaseRqlParser";
import { AliasNameContext } from "./BaseRqlParser";
import { PrealiasContext } from "./BaseRqlParser";
import { AsArrayContext } from "./BaseRqlParser";
import { IncludeModeContext } from "./BaseRqlParser";
import { IncludeStatementContext } from "./BaseRqlParser";
import { LimitStatementContext } from "./BaseRqlParser";
import { VariableContext } from "./BaseRqlParser";
import { MemberNameContext } from "./BaseRqlParser";
import { ParamContext } from "./BaseRqlParser";
import { LiteralContext } from "./BaseRqlParser";
import { CacheParamContext } from "./BaseRqlParser";
import { ParameterWithOptionalAliasContext } from "./BaseRqlParser";
import { VariableOrFunctionContext } from "./BaseRqlParser";
import { FunctionContext } from "./BaseRqlParser";
import { ArgumentsContext } from "./BaseRqlParser";
import { IdentifiersWithoutRootKeywordsContext } from "./BaseRqlParser";
import { RootKeywordsContext } from "./BaseRqlParser";
import { IdentifiersAllNamesContext } from "./BaseRqlParser";
import { DateContext } from "./BaseRqlParser";
import { DateStringContext } from "./BaseRqlParser";
import { TsProgContext } from "./BaseRqlParser";
import { TsIncludeTimeseriesFunctionContext } from "./BaseRqlParser";
import { TsIncludeLiteralContext } from "./BaseRqlParser";
import { TsIncludeSpecialMethodContext } from "./BaseRqlParser";
import { TsQueryBodyContext } from "./BaseRqlParser";
import { TsOffsetContext } from "./BaseRqlParser";
import { TsFunctionContext } from "./BaseRqlParser";
import { TsTimeRangeStatementContext } from "./BaseRqlParser";
import { TsLoadStatementContext } from "./BaseRqlParser";
import { TsAliasContext } from "./BaseRqlParser";
import { TsFROMContext } from "./BaseRqlParser";
import { TsWHEREContext } from "./BaseRqlParser";
import { TsExprContext } from "./BaseRqlParser";
import { TsBetweenContext } from "./BaseRqlParser";
import { TsBinaryContext } from "./BaseRqlParser";
import { TsLiteralContext } from "./BaseRqlParser";
import { TsTimeRangeFirstContext } from "./BaseRqlParser";
import { TsTimeRangeLastContext } from "./BaseRqlParser";
import { TsCollectionNameContext } from "./BaseRqlParser";
import { TsGroupByContext } from "./BaseRqlParser";
import { TsSelectContext } from "./BaseRqlParser";
import { TsSelectScaleProjectionContext } from "./BaseRqlParser";
import { TsSelectVariableContext } from "./BaseRqlParser";
import { TsIdentifiersContext } from "./BaseRqlParser";
import { UpdateBodyContext } from "./BaseRqlParser";
import { FilterStatementContext } from "./BaseRqlParser";
import { FilterExprContext } from "./BaseRqlParser";
import { FilterModeContext } from "./BaseRqlParser";
import { ParameterBeforeQueryContext } from "./BaseRqlParser";
import { ParameterValueContext } from "./BaseRqlParser";
import { JsonContext } from "./BaseRqlParser";
import { JsonObjContext } from "./BaseRqlParser";
import { JsonPairContext } from "./BaseRqlParser";
import { JsonArrContext } from "./BaseRqlParser";
import { JsonValueContext } from "./BaseRqlParser";
import { StringContext } from "./BaseRqlParser";


/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by `BaseRqlParser`.
 *
 * @param <Result> The return type of the visit operation. Use `void` for
 * operations with no return type.
 */
export interface BaseRqlParserVisitor<Result> extends ParseTreeVisitor<Result> {
	/**
	 * Visit a parse tree produced by the `CollectionByIndex`
	 * labeled alternative in `BaseRqlParser.fromStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitCollectionByIndex?: (ctx: CollectionByIndexContext) => Result;

	/**
	 * Visit a parse tree produced by the `AllCollections`
	 * labeled alternative in `BaseRqlParser.fromStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitAllCollections?: (ctx: AllCollectionsContext) => Result;

	/**
	 * Visit a parse tree produced by the `CollectionByName`
	 * labeled alternative in `BaseRqlParser.fromStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitCollectionByName?: (ctx: CollectionByNameContext) => Result;

	/**
	 * Visit a parse tree produced by the `getAllDistinct`
	 * labeled alternative in `BaseRqlParser.selectStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitGetAllDistinct?: (ctx: GetAllDistinctContext) => Result;

	/**
	 * Visit a parse tree produced by the `ProjectIndividualFields`
	 * labeled alternative in `BaseRqlParser.selectStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitProjectIndividualFields?: (ctx: ProjectIndividualFieldsContext) => Result;

	/**
	 * Visit a parse tree produced by the `javascriptCode`
	 * labeled alternative in `BaseRqlParser.selectStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitJavascriptCode?: (ctx: JavascriptCodeContext) => Result;

	/**
	 * Visit a parse tree produced by the `parameterExpr`
	 * labeled alternative in `BaseRqlParser.exprValue`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitParameterExpr?: (ctx: ParameterExprContext) => Result;

	/**
	 * Visit a parse tree produced by the `binaryExpression`
	 * labeled alternative in `BaseRqlParser.expr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitBinaryExpression?: (ctx: BinaryExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `opPar`
	 * labeled alternative in `BaseRqlParser.expr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitOpPar?: (ctx: OpParContext) => Result;

	/**
	 * Visit a parse tree produced by the `equalExpression`
	 * labeled alternative in `BaseRqlParser.expr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitEqualExpression?: (ctx: EqualExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `mathExpression`
	 * labeled alternative in `BaseRqlParser.expr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitMathExpression?: (ctx: MathExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `specialFunctionst`
	 * labeled alternative in `BaseRqlParser.expr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitSpecialFunctionst?: (ctx: SpecialFunctionstContext) => Result;

	/**
	 * Visit a parse tree produced by the `inExpr`
	 * labeled alternative in `BaseRqlParser.expr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitInExpr?: (ctx: InExprContext) => Result;

	/**
	 * Visit a parse tree produced by the `betweenExpr`
	 * labeled alternative in `BaseRqlParser.expr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitBetweenExpr?: (ctx: BetweenExprContext) => Result;

	/**
	 * Visit a parse tree produced by the `normalFunc`
	 * labeled alternative in `BaseRqlParser.expr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitNormalFunc?: (ctx: NormalFuncContext) => Result;

	/**
	 * Visit a parse tree produced by the `booleanExpression`
	 * labeled alternative in `BaseRqlParser.expr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitBooleanExpression?: (ctx: BooleanExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `filterBinaryExpression`
	 * labeled alternative in `BaseRqlParser.filterExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFilterBinaryExpression?: (ctx: FilterBinaryExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `filterOpPar`
	 * labeled alternative in `BaseRqlParser.filterExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFilterOpPar?: (ctx: FilterOpParContext) => Result;

	/**
	 * Visit a parse tree produced by the `filterEqualExpression`
	 * labeled alternative in `BaseRqlParser.filterExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFilterEqualExpression?: (ctx: FilterEqualExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `filterMathExpression`
	 * labeled alternative in `BaseRqlParser.filterExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFilterMathExpression?: (ctx: FilterMathExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `filterNormalFunc`
	 * labeled alternative in `BaseRqlParser.filterExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFilterNormalFunc?: (ctx: FilterNormalFuncContext) => Result;

	/**
	 * Visit a parse tree produced by the `filterBooleanExpression`
	 * labeled alternative in `BaseRqlParser.filterExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFilterBooleanExpression?: (ctx: FilterBooleanExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `javaScriptFunction`
	 * labeled alternative in `BaseRqlParser.functionStatment`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitJavaScriptFunction?: (ctx: JavaScriptFunctionContext) => Result;

	/**
	 * Visit a parse tree produced by the `timeSeriesFunction`
	 * labeled alternative in `BaseRqlParser.functionStatment`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTimeSeriesFunction?: (ctx: TimeSeriesFunctionContext) => Result;

	/**
	 * Visit a parse tree produced by the `tsMathExpression`
	 * labeled alternative in `BaseRqlParser.tsExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsMathExpression?: (ctx: TsMathExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `tsBinaryExpression`
	 * labeled alternative in `BaseRqlParser.tsExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsBinaryExpression?: (ctx: TsBinaryExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `tsOpPar`
	 * labeled alternative in `BaseRqlParser.tsExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsOpPar?: (ctx: TsOpParContext) => Result;

	/**
	 * Visit a parse tree produced by the `tsBooleanExpression`
	 * labeled alternative in `BaseRqlParser.tsExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsBooleanExpression?: (ctx: TsBooleanExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by the `tsLiteralExpression`
	 * labeled alternative in `BaseRqlParser.tsExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsLiteralExpression?: (ctx: TsLiteralExpressionContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.prog`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitProg?: (ctx: ProgContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.functionStatment`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFunctionStatment?: (ctx: FunctionStatmentContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.updateStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitUpdateStatement?: (ctx: UpdateStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.fromMode`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFromMode?: (ctx: FromModeContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.fromStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFromStatement?: (ctx: FromStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.indexName`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitIndexName?: (ctx: IndexNameContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.collectionName`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitCollectionName?: (ctx: CollectionNameContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.aliasWithOptionalAs`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitAliasWithOptionalAs?: (ctx: AliasWithOptionalAsContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.groupByMode`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitGroupByMode?: (ctx: GroupByModeContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.groupByStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitGroupByStatement?: (ctx: GroupByStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.suggestGroupBy`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitSuggestGroupBy?: (ctx: SuggestGroupByContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.whereMode`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitWhereMode?: (ctx: WhereModeContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.whereStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitWhereStatement?: (ctx: WhereStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.expr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitExpr?: (ctx: ExprContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.binary`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitBinary?: (ctx: BinaryContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.exprValue`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitExprValue?: (ctx: ExprValueContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.inFunction`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitInFunction?: (ctx: InFunctionContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.betweenFunction`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitBetweenFunction?: (ctx: BetweenFunctionContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.specialFunctions`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitSpecialFunctions?: (ctx: SpecialFunctionsContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.specialFunctionName`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitSpecialFunctionName?: (ctx: SpecialFunctionNameContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.specialParam`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitSpecialParam?: (ctx: SpecialParamContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.loadMode`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitLoadMode?: (ctx: LoadModeContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.loadStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitLoadStatement?: (ctx: LoadStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.loadDocumentByName`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitLoadDocumentByName?: (ctx: LoadDocumentByNameContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.orderByMode`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitOrderByMode?: (ctx: OrderByModeContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.orderByStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitOrderByStatement?: (ctx: OrderByStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.orderByItem`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitOrderByItem?: (ctx: OrderByItemContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.orderBySorting`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitOrderBySorting?: (ctx: OrderBySortingContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.orderBySortingAs`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitOrderBySortingAs?: (ctx: OrderBySortingAsContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.orderByOrder`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitOrderByOrder?: (ctx: OrderByOrderContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.selectMode`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitSelectMode?: (ctx: SelectModeContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.selectStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitSelectStatement?: (ctx: SelectStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.projectField`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitProjectField?: (ctx: ProjectFieldContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.jsFunction`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitJsFunction?: (ctx: JsFunctionContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.jsBody`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitJsBody?: (ctx: JsBodyContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.aliasWithRequiredAs`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitAliasWithRequiredAs?: (ctx: AliasWithRequiredAsContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.aliasName`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitAliasName?: (ctx: AliasNameContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.prealias`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitPrealias?: (ctx: PrealiasContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.asArray`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitAsArray?: (ctx: AsArrayContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.includeMode`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitIncludeMode?: (ctx: IncludeModeContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.includeStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitIncludeStatement?: (ctx: IncludeStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.limitStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitLimitStatement?: (ctx: LimitStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.variable`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitVariable?: (ctx: VariableContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.memberName`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitMemberName?: (ctx: MemberNameContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.param`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitParam?: (ctx: ParamContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.literal`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitLiteral?: (ctx: LiteralContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.cacheParam`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitCacheParam?: (ctx: CacheParamContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.parameterWithOptionalAlias`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitParameterWithOptionalAlias?: (ctx: ParameterWithOptionalAliasContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.variableOrFunction`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitVariableOrFunction?: (ctx: VariableOrFunctionContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.function`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFunction?: (ctx: FunctionContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.arguments`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitArguments?: (ctx: ArgumentsContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.identifiersWithoutRootKeywords`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitIdentifiersWithoutRootKeywords?: (ctx: IdentifiersWithoutRootKeywordsContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.rootKeywords`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitRootKeywords?: (ctx: RootKeywordsContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.identifiersAllNames`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitIdentifiersAllNames?: (ctx: IdentifiersAllNamesContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.date`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitDate?: (ctx: DateContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.dateString`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitDateString?: (ctx: DateStringContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsProg`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsProg?: (ctx: TsProgContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsIncludeTimeseriesFunction`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsIncludeTimeseriesFunction?: (ctx: TsIncludeTimeseriesFunctionContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsIncludeLiteral`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsIncludeLiteral?: (ctx: TsIncludeLiteralContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsIncludeSpecialMethod`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsIncludeSpecialMethod?: (ctx: TsIncludeSpecialMethodContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsQueryBody`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsQueryBody?: (ctx: TsQueryBodyContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsOffset`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsOffset?: (ctx: TsOffsetContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsFunction`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsFunction?: (ctx: TsFunctionContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsTimeRangeStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsTimeRangeStatement?: (ctx: TsTimeRangeStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsLoadStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsLoadStatement?: (ctx: TsLoadStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsAlias`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsAlias?: (ctx: TsAliasContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsFROM`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsFROM?: (ctx: TsFROMContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsWHERE`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsWHERE?: (ctx: TsWHEREContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsExpr?: (ctx: TsExprContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsBetween`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsBetween?: (ctx: TsBetweenContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsBinary`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsBinary?: (ctx: TsBinaryContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsLiteral`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsLiteral?: (ctx: TsLiteralContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsTimeRangeFirst`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsTimeRangeFirst?: (ctx: TsTimeRangeFirstContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsTimeRangeLast`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsTimeRangeLast?: (ctx: TsTimeRangeLastContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsCollectionName`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsCollectionName?: (ctx: TsCollectionNameContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsGroupBy`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsGroupBy?: (ctx: TsGroupByContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsSelect`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsSelect?: (ctx: TsSelectContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsSelectScaleProjection`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsSelectScaleProjection?: (ctx: TsSelectScaleProjectionContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsSelectVariable`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsSelectVariable?: (ctx: TsSelectVariableContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.tsIdentifiers`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitTsIdentifiers?: (ctx: TsIdentifiersContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.updateBody`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitUpdateBody?: (ctx: UpdateBodyContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.filterStatement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFilterStatement?: (ctx: FilterStatementContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.filterExpr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFilterExpr?: (ctx: FilterExprContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.filterMode`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFilterMode?: (ctx: FilterModeContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.parameterBeforeQuery`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitParameterBeforeQuery?: (ctx: ParameterBeforeQueryContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.parameterValue`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitParameterValue?: (ctx: ParameterValueContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.json`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitJson?: (ctx: JsonContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.jsonObj`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitJsonObj?: (ctx: JsonObjContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.jsonPair`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitJsonPair?: (ctx: JsonPairContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.jsonArr`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitJsonArr?: (ctx: JsonArrContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.jsonValue`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitJsonValue?: (ctx: JsonValueContext) => Result;

	/**
	 * Visit a parse tree produced by `BaseRqlParser.string`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitString?: (ctx: StringContext) => Result;
}

