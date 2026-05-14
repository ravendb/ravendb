// Generated from languageService/grammar/BaseRqlParser.g4 by ANTLR 4.9.0-SNAPSHOT


import { ATN } from "antlr4ts/atn/ATN";
import { ATNDeserializer } from "antlr4ts/atn/ATNDeserializer";
import { FailedPredicateException } from "antlr4ts/FailedPredicateException";
import { NotNull } from "antlr4ts/Decorators";
import { NoViableAltException } from "antlr4ts/NoViableAltException";
import { Override } from "antlr4ts/Decorators";
import { Parser } from "antlr4ts/Parser";
import { ParserRuleContext } from "antlr4ts/ParserRuleContext";
import { ParserATNSimulator } from "antlr4ts/atn/ParserATNSimulator";
import { ParseTreeListener } from "antlr4ts/tree/ParseTreeListener";
import { ParseTreeVisitor } from "antlr4ts/tree/ParseTreeVisitor";
import { RecognitionException } from "antlr4ts/RecognitionException";
import { RuleContext } from "antlr4ts/RuleContext";
//import { RuleVersion } from "antlr4ts/RuleVersion";
import { TerminalNode } from "antlr4ts/tree/TerminalNode";
import { Token } from "antlr4ts/Token";
import { TokenStream } from "antlr4ts/TokenStream";
import { Vocabulary } from "antlr4ts/Vocabulary";
import { VocabularyImpl } from "antlr4ts/VocabularyImpl";

import * as Utils from "antlr4ts/misc/Utils";

import { BaseRqlParserVisitor } from "./BaseRqlParserVisitor";


export class BaseRqlParser extends Parser {
	public static readonly CL_CUR = 1;
	public static readonly CL_PAR = 2;
	public static readonly CL_Q = 3;
	public static readonly COMMA = 4;
	public static readonly DOT = 5;
	public static readonly EQUAL = 6;
	public static readonly MATH = 7;
	public static readonly OP_CUR = 8;
	public static readonly OP_PAR = 9;
	public static readonly OP_Q = 10;
	public static readonly SLASH = 11;
	public static readonly COLON = 12;
	public static readonly SEMICOLON = 13;
	public static readonly BACKSLASH = 14;
	public static readonly PLUS = 15;
	public static readonly MINUS = 16;
	public static readonly AT = 17;
	public static readonly HASH = 18;
	public static readonly DOL = 19;
	public static readonly PERCENT = 20;
	public static readonly POWER = 21;
	public static readonly AMP = 22;
	public static readonly STAR = 23;
	public static readonly QUESTION_MARK = 24;
	public static readonly EXCLAMATION = 25;
	public static readonly ALL = 26;
	public static readonly ALL_DOCS = 27;
	public static readonly ALPHANUMERIC = 28;
	public static readonly AND = 29;
	public static readonly AS = 30;
	public static readonly BETWEEN = 31;
	public static readonly DISTINCT = 32;
	public static readonly DOUBLE = 33;
	public static readonly ENDS_WITH = 34;
	public static readonly STARTS_WITH = 35;
	public static readonly FALSE = 36;
	public static readonly FACET = 37;
	public static readonly FROM = 38;
	public static readonly GROUP_BY = 39;
	public static readonly ID = 40;
	public static readonly IN = 41;
	public static readonly INCLUDE = 42;
	public static readonly UPDATE = 43;
	public static readonly INDEX = 44;
	public static readonly INTERSECT = 45;
	public static readonly LOAD = 46;
	public static readonly LONG = 47;
	public static readonly MATCH = 48;
	public static readonly METADATA = 49;
	public static readonly MORELIKETHIS = 50;
	public static readonly NOT = 51;
	public static readonly NULL = 52;
	public static readonly OR = 53;
	public static readonly ORDER_BY = 54;
	public static readonly NULLS_FIRST = 55;
	public static readonly NULLS_LAST = 56;
	public static readonly OFFSET = 57;
	public static readonly SELECT = 58;
	public static readonly JS_SELECT = 59;
	public static readonly SORTING = 60;
	public static readonly STRING_W = 61;
	public static readonly TO = 62;
	public static readonly TRUE = 63;
	public static readonly WHERE = 64;
	public static readonly WITH = 65;
	public static readonly EXACT = 66;
	public static readonly BOOST = 67;
	public static readonly SEARCH = 68;
	public static readonly VECTOR_SEARCH = 69;
	public static readonly EMBEDDING_ARRAY = 70;
	public static readonly EMBEDDING_TEXT = 71;
	public static readonly EMBEDDING_FOR = 72;
	public static readonly AI_TASK = 73;
	public static readonly LIMIT = 74;
	public static readonly FUZZY = 75;
	public static readonly FILTER = 76;
	public static readonly FILTER_LIMIT = 77;
	public static readonly TIMESERIES = 78;
	public static readonly JS_FUNCTION_DECLARATION = 79;
	public static readonly TIMESERIES_FUNCTION_DECLARATION = 80;
	public static readonly NUM = 81;
	public static readonly DOUBLE_QUOTE_STRING = 82;
	public static readonly SINGLE_QUOTE_STRING = 83;
	public static readonly WORD = 84;
	public static readonly WS = 85;
	public static readonly TS_METHOD = 86;
	public static readonly TS_OP_C = 87;
	public static readonly TS_CL_C = 88;
	public static readonly TS_OP_PAR = 89;
	public static readonly TS_CL_PAR = 90;
	public static readonly TS_OP_Q = 91;
	public static readonly TS_CL_Q = 92;
	public static readonly TS_DOT = 93;
	public static readonly TS_COMMA = 94;
	public static readonly TS_DOL = 95;
	public static readonly TS_MATH = 96;
	public static readonly TS_OR = 97;
	public static readonly TS_TRUE = 98;
	public static readonly TS_NOT = 99;
	public static readonly TS_AS = 100;
	public static readonly TS_AND = 101;
	public static readonly TS_FROM = 102;
	public static readonly TS_WHERE = 103;
	public static readonly TS_GROUPBY = 104;
	public static readonly TS_BETWEEN = 105;
	public static readonly TS_FIRST = 106;
	public static readonly TS_LAST = 107;
	public static readonly TS_WITH = 108;
	public static readonly TS_TIMERANGE = 109;
	public static readonly TS_GROUPBY_VALUE = 110;
	public static readonly TS_SELECT = 111;
	public static readonly TS_LOAD = 112;
	public static readonly TS_SCALE = 113;
	public static readonly TS_OFFSET = 114;
	public static readonly TS_NUM = 115;
	public static readonly TS_STRING = 116;
	public static readonly TS_SINGLE_QUOTE_STRING = 117;
	public static readonly TS_WORD = 118;
	public static readonly TS_WS = 119;
	public static readonly US_OP = 120;
	public static readonly US_CL = 121;
	public static readonly US_WS = 122;
	public static readonly US_DATA = 123;
	public static readonly JS_OP = 124;
	public static readonly JS_CL = 125;
	public static readonly JS_DATA = 126;
	public static readonly JFN_WORD = 127;
	public static readonly JFN_OP_PAR = 128;
	public static readonly JFN_CL_PAR = 129;
	public static readonly JFN_OP_JS = 130;
	public static readonly JFN_COMMA = 131;
	public static readonly JFN_WS = 132;
	public static readonly RULE_prog = 0;
	public static readonly RULE_functionStatment = 1;
	public static readonly RULE_updateStatement = 2;
	public static readonly RULE_fromMode = 3;
	public static readonly RULE_fromStatement = 4;
	public static readonly RULE_indexName = 5;
	public static readonly RULE_collectionName = 6;
	public static readonly RULE_aliasWithOptionalAs = 7;
	public static readonly RULE_groupByMode = 8;
	public static readonly RULE_groupByStatement = 9;
	public static readonly RULE_suggestGroupBy = 10;
	public static readonly RULE_whereMode = 11;
	public static readonly RULE_whereStatement = 12;
	public static readonly RULE_expr = 13;
	public static readonly RULE_binary = 14;
	public static readonly RULE_exprValue = 15;
	public static readonly RULE_inFunction = 16;
	public static readonly RULE_betweenFunction = 17;
	public static readonly RULE_specialFunctions = 18;
	public static readonly RULE_specialFunctionName = 19;
	public static readonly RULE_vectorSearch = 20;
	public static readonly RULE_vectorSearchParam = 21;
	public static readonly RULE_aiTaskMethod = 22;
	public static readonly RULE_vectorSearchAdditionalParams = 23;
	public static readonly RULE_vectorSearchValue = 24;
	public static readonly RULE_specialParam = 25;
	public static readonly RULE_loadMode = 26;
	public static readonly RULE_loadStatement = 27;
	public static readonly RULE_loadDocumentByName = 28;
	public static readonly RULE_orderByMode = 29;
	public static readonly RULE_orderByStatement = 30;
	public static readonly RULE_orderByItem = 31;
	public static readonly RULE_orderBySorting = 32;
	public static readonly RULE_orderBySortingAs = 33;
	public static readonly RULE_orderByOrder = 34;
	public static readonly RULE_orderByNulls = 35;
	public static readonly RULE_selectMode = 36;
	public static readonly RULE_selectStatement = 37;
	public static readonly RULE_projectField = 38;
	public static readonly RULE_jsFunction = 39;
	public static readonly RULE_jsBody = 40;
	public static readonly RULE_aliasWithRequiredAs = 41;
	public static readonly RULE_aliasName = 42;
	public static readonly RULE_prealias = 43;
	public static readonly RULE_asArray = 44;
	public static readonly RULE_includeMode = 45;
	public static readonly RULE_includeStatement = 46;
	public static readonly RULE_limitStatement = 47;
	public static readonly RULE_variable = 48;
	public static readonly RULE_memberName = 49;
	public static readonly RULE_param = 50;
	public static readonly RULE_literal = 51;
	public static readonly RULE_cacheParam = 52;
	public static readonly RULE_parameterWithOptionalAlias = 53;
	public static readonly RULE_variableOrFunction = 54;
	public static readonly RULE_function = 55;
	public static readonly RULE_arguments = 56;
	public static readonly RULE_identifiersWithoutRootKeywords = 57;
	public static readonly RULE_rootKeywords = 58;
	public static readonly RULE_identifiersAllNames = 59;
	public static readonly RULE_date = 60;
	public static readonly RULE_dateString = 61;
	public static readonly RULE_tsProg = 62;
	public static readonly RULE_tsIncludeTimeseriesFunction = 63;
	public static readonly RULE_tsIncludeLiteral = 64;
	public static readonly RULE_tsIncludeSpecialMethod = 65;
	public static readonly RULE_tsQueryBody = 66;
	public static readonly RULE_tsOffset = 67;
	public static readonly RULE_tsFunction = 68;
	public static readonly RULE_tsTimeRangeStatement = 69;
	public static readonly RULE_tsLoadStatement = 70;
	public static readonly RULE_tsAlias = 71;
	public static readonly RULE_tsFROM = 72;
	public static readonly RULE_tsWHERE = 73;
	public static readonly RULE_tsExpr = 74;
	public static readonly RULE_tsBetween = 75;
	public static readonly RULE_tsBinary = 76;
	public static readonly RULE_tsLiteral = 77;
	public static readonly RULE_tsTimeRangeFirst = 78;
	public static readonly RULE_tsTimeRangeLast = 79;
	public static readonly RULE_tsCollectionName = 80;
	public static readonly RULE_tsGroupBy = 81;
	public static readonly RULE_tsSelect = 82;
	public static readonly RULE_tsSelectScaleProjection = 83;
	public static readonly RULE_tsSelectVariable = 84;
	public static readonly RULE_tsIdentifiers = 85;
	public static readonly RULE_updateBody = 86;
	public static readonly RULE_filterStatement = 87;
	public static readonly RULE_filterExpr = 88;
	public static readonly RULE_filterMode = 89;
	public static readonly RULE_parameterBeforeQuery = 90;
	public static readonly RULE_parameterValue = 91;
	public static readonly RULE_json = 92;
	public static readonly RULE_jsonObj = 93;
	public static readonly RULE_jsonPair = 94;
	public static readonly RULE_jsonArr = 95;
	public static readonly RULE_jsonValue = 96;
	public static readonly RULE_string = 97;
	// tslint:disable:no-trailing-whitespace
	public static readonly ruleNames: string[] = [
		"prog", "functionStatment", "updateStatement", "fromMode", "fromStatement", 
		"indexName", "collectionName", "aliasWithOptionalAs", "groupByMode", "groupByStatement", 
		"suggestGroupBy", "whereMode", "whereStatement", "expr", "binary", "exprValue", 
		"inFunction", "betweenFunction", "specialFunctions", "specialFunctionName", 
		"vectorSearch", "vectorSearchParam", "aiTaskMethod", "vectorSearchAdditionalParams", 
		"vectorSearchValue", "specialParam", "loadMode", "loadStatement", "loadDocumentByName", 
		"orderByMode", "orderByStatement", "orderByItem", "orderBySorting", "orderBySortingAs", 
		"orderByOrder", "orderByNulls", "selectMode", "selectStatement", "projectField", 
		"jsFunction", "jsBody", "aliasWithRequiredAs", "aliasName", "prealias", 
		"asArray", "includeMode", "includeStatement", "limitStatement", "variable", 
		"memberName", "param", "literal", "cacheParam", "parameterWithOptionalAlias", 
		"variableOrFunction", "function", "arguments", "identifiersWithoutRootKeywords", 
		"rootKeywords", "identifiersAllNames", "date", "dateString", "tsProg", 
		"tsIncludeTimeseriesFunction", "tsIncludeLiteral", "tsIncludeSpecialMethod", 
		"tsQueryBody", "tsOffset", "tsFunction", "tsTimeRangeStatement", "tsLoadStatement", 
		"tsAlias", "tsFROM", "tsWHERE", "tsExpr", "tsBetween", "tsBinary", "tsLiteral", 
		"tsTimeRangeFirst", "tsTimeRangeLast", "tsCollectionName", "tsGroupBy", 
		"tsSelect", "tsSelectScaleProjection", "tsSelectVariable", "tsIdentifiers", 
		"updateBody", "filterStatement", "filterExpr", "filterMode", "parameterBeforeQuery", 
		"parameterValue", "json", "jsonObj", "jsonPair", "jsonArr", "jsonValue", 
		"string",
	];

	private static readonly _LITERAL_NAMES: Array<string | undefined> = [
		undefined, undefined, undefined, undefined, undefined, undefined, undefined, 
		undefined, undefined, undefined, undefined, "'/'", "':'", "';'", undefined, 
		"'+'", "'-'", "'@'", "'#'", undefined, "'%'", "'^'", "'&'", "'*'", "'?'", 
		"'!'", undefined, "'@all_docs'",
	];
	private static readonly _SYMBOLIC_NAMES: Array<string | undefined> = [
		undefined, "CL_CUR", "CL_PAR", "CL_Q", "COMMA", "DOT", "EQUAL", "MATH", 
		"OP_CUR", "OP_PAR", "OP_Q", "SLASH", "COLON", "SEMICOLON", "BACKSLASH", 
		"PLUS", "MINUS", "AT", "HASH", "DOL", "PERCENT", "POWER", "AMP", "STAR", 
		"QUESTION_MARK", "EXCLAMATION", "ALL", "ALL_DOCS", "ALPHANUMERIC", "AND", 
		"AS", "BETWEEN", "DISTINCT", "DOUBLE", "ENDS_WITH", "STARTS_WITH", "FALSE", 
		"FACET", "FROM", "GROUP_BY", "ID", "IN", "INCLUDE", "UPDATE", "INDEX", 
		"INTERSECT", "LOAD", "LONG", "MATCH", "METADATA", "MORELIKETHIS", "NOT", 
		"NULL", "OR", "ORDER_BY", "NULLS_FIRST", "NULLS_LAST", "OFFSET", "SELECT", 
		"JS_SELECT", "SORTING", "STRING_W", "TO", "TRUE", "WHERE", "WITH", "EXACT", 
		"BOOST", "SEARCH", "VECTOR_SEARCH", "EMBEDDING_ARRAY", "EMBEDDING_TEXT", 
		"EMBEDDING_FOR", "AI_TASK", "LIMIT", "FUZZY", "FILTER", "FILTER_LIMIT", 
		"TIMESERIES", "JS_FUNCTION_DECLARATION", "TIMESERIES_FUNCTION_DECLARATION", 
		"NUM", "DOUBLE_QUOTE_STRING", "SINGLE_QUOTE_STRING", "WORD", "WS", "TS_METHOD", 
		"TS_OP_C", "TS_CL_C", "TS_OP_PAR", "TS_CL_PAR", "TS_OP_Q", "TS_CL_Q", 
		"TS_DOT", "TS_COMMA", "TS_DOL", "TS_MATH", "TS_OR", "TS_TRUE", "TS_NOT", 
		"TS_AS", "TS_AND", "TS_FROM", "TS_WHERE", "TS_GROUPBY", "TS_BETWEEN", 
		"TS_FIRST", "TS_LAST", "TS_WITH", "TS_TIMERANGE", "TS_GROUPBY_VALUE", 
		"TS_SELECT", "TS_LOAD", "TS_SCALE", "TS_OFFSET", "TS_NUM", "TS_STRING", 
		"TS_SINGLE_QUOTE_STRING", "TS_WORD", "TS_WS", "US_OP", "US_CL", "US_WS", 
		"US_DATA", "JS_OP", "JS_CL", "JS_DATA", "JFN_WORD", "JFN_OP_PAR", "JFN_CL_PAR", 
		"JFN_OP_JS", "JFN_COMMA", "JFN_WS",
	];
	public static readonly VOCABULARY: Vocabulary = new VocabularyImpl(BaseRqlParser._LITERAL_NAMES, BaseRqlParser._SYMBOLIC_NAMES, []);

	// @Override
	// @NotNull
	public get vocabulary(): Vocabulary {
		return BaseRqlParser.VOCABULARY;
	}
	// tslint:enable:no-trailing-whitespace

	// @Override
	public get grammarFileName(): string { return "BaseRqlParser.g4"; }

	// @Override
	public get ruleNames(): string[] { return BaseRqlParser.ruleNames; }

	// @Override
	public get serializedATN(): string { return BaseRqlParser._serializedATN; }

	protected createFailedPredicateException(predicate?: string, message?: string): FailedPredicateException {
		return new FailedPredicateException(this, predicate, message);
	}

	constructor(input: TokenStream) {
		super(input);
		this._interp = new ParserATNSimulator(BaseRqlParser._ATN, this);
	}
	// @RuleVersion(0)
	public prog(): ProgContext {
		let _localctx: ProgContext = new ProgContext(this._ctx, this.state);
		this.enterRule(_localctx, 0, BaseRqlParser.RULE_prog);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 199;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 0, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 196;
					this.parameterBeforeQuery();
					}
					}
				}
				this.state = 201;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 0, this._ctx);
			}
			this.state = 205;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.JS_FUNCTION_DECLARATION || _la === BaseRqlParser.TIMESERIES_FUNCTION_DECLARATION) {
				{
				{
				this.state = 202;
				this.functionStatment();
				}
				}
				this.state = 207;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 208;
			this.fromStatement();
			this.state = 210;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.GROUP_BY) {
				{
				this.state = 209;
				this.groupByStatement();
				}
			}

			this.state = 213;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.WHERE) {
				{
				this.state = 212;
				this.whereStatement();
				}
			}

			this.state = 216;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.ORDER_BY) {
				{
				this.state = 215;
				this.orderByStatement();
				}
			}

			this.state = 219;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.LOAD) {
				{
				this.state = 218;
				this.loadStatement();
				}
			}

			this.state = 222;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.UPDATE) {
				{
				this.state = 221;
				this.updateStatement();
				}
			}

			this.state = 225;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.FILTER) {
				{
				this.state = 224;
				this.filterStatement();
				}
			}

			this.state = 228;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.SELECT || _la === BaseRqlParser.JS_SELECT) {
				{
				this.state = 227;
				this.selectStatement();
				}
			}

			this.state = 231;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.INCLUDE) {
				{
				this.state = 230;
				this.includeStatement();
				}
			}

			this.state = 234;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.LIMIT || _la === BaseRqlParser.FILTER_LIMIT) {
				{
				this.state = 233;
				this.limitStatement();
				}
			}

			this.state = 237;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.OP_CUR) {
				{
				this.state = 236;
				this.json();
				}
			}

			this.state = 239;
			this.match(BaseRqlParser.EOF);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public functionStatment(): FunctionStatmentContext {
		let _localctx: FunctionStatmentContext = new FunctionStatmentContext(this._ctx, this.state);
		this.enterRule(_localctx, 2, BaseRqlParser.RULE_functionStatment);
		try {
			this.state = 243;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.JS_FUNCTION_DECLARATION:
				_localctx = new JavaScriptFunctionContext(_localctx);
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 241;
				this.jsFunction();
				}
				break;
			case BaseRqlParser.TIMESERIES_FUNCTION_DECLARATION:
				_localctx = new TimeSeriesFunctionContext(_localctx);
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 242;
				this.tsFunction();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public updateStatement(): UpdateStatementContext {
		let _localctx: UpdateStatementContext = new UpdateStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 4, BaseRqlParser.RULE_updateStatement);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 245;
			this.match(BaseRqlParser.UPDATE);
			this.state = 246;
			this.match(BaseRqlParser.US_OP);
			this.state = 250;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.US_OP) {
				{
				{
				this.state = 247;
				this.updateBody();
				}
				}
				this.state = 252;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 253;
			this.match(BaseRqlParser.US_CL);
			this.state = 254;
			this.match(BaseRqlParser.EOF);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public fromMode(): FromModeContext {
		let _localctx: FromModeContext = new FromModeContext(this._ctx, this.state);
		this.enterRule(_localctx, 6, BaseRqlParser.RULE_fromMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 256;
			this.match(BaseRqlParser.FROM);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public fromStatement(): FromStatementContext {
		let _localctx: FromStatementContext = new FromStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 8, BaseRqlParser.RULE_fromStatement);
		let _la: number;
		try {
			this.state = 271;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 16, this._ctx) ) {
			case 1:
				_localctx = new CollectionByIndexContext(_localctx);
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 258;
				this.fromMode();
				this.state = 259;
				this.match(BaseRqlParser.INDEX);
				this.state = 260;
				(_localctx as CollectionByIndexContext)._collection = this.indexName();
				this.state = 262;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (((((_la - 26)) & ~0x1F) === 0 && ((1 << (_la - 26)) & ((1 << (BaseRqlParser.ALL - 26)) | (1 << (BaseRqlParser.ALPHANUMERIC - 26)) | (1 << (BaseRqlParser.AND - 26)) | (1 << (BaseRqlParser.AS - 26)) | (1 << (BaseRqlParser.BETWEEN - 26)) | (1 << (BaseRqlParser.DISTINCT - 26)) | (1 << (BaseRqlParser.DOUBLE - 26)) | (1 << (BaseRqlParser.ENDS_WITH - 26)) | (1 << (BaseRqlParser.STARTS_WITH - 26)) | (1 << (BaseRqlParser.FALSE - 26)) | (1 << (BaseRqlParser.FACET - 26)) | (1 << (BaseRqlParser.ID - 26)) | (1 << (BaseRqlParser.IN - 26)) | (1 << (BaseRqlParser.INTERSECT - 26)) | (1 << (BaseRqlParser.LONG - 26)) | (1 << (BaseRqlParser.MATCH - 26)) | (1 << (BaseRqlParser.METADATA - 26)) | (1 << (BaseRqlParser.MORELIKETHIS - 26)) | (1 << (BaseRqlParser.NOT - 26)) | (1 << (BaseRqlParser.NULL - 26)) | (1 << (BaseRqlParser.OR - 26)))) !== 0) || ((((_la - 60)) & ~0x1F) === 0 && ((1 << (_la - 60)) & ((1 << (BaseRqlParser.SORTING - 60)) | (1 << (BaseRqlParser.STRING_W - 60)) | (1 << (BaseRqlParser.TO - 60)) | (1 << (BaseRqlParser.TRUE - 60)) | (1 << (BaseRqlParser.WITH - 60)) | (1 << (BaseRqlParser.EXACT - 60)) | (1 << (BaseRqlParser.BOOST - 60)) | (1 << (BaseRqlParser.SEARCH - 60)) | (1 << (BaseRqlParser.FUZZY - 60)) | (1 << (BaseRqlParser.DOUBLE_QUOTE_STRING - 60)) | (1 << (BaseRqlParser.SINGLE_QUOTE_STRING - 60)) | (1 << (BaseRqlParser.WORD - 60)))) !== 0)) {
					{
					this.state = 261;
					this.aliasWithOptionalAs();
					}
				}

				}
				break;

			case 2:
				_localctx = new AllCollectionsContext(_localctx);
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 264;
				this.match(BaseRqlParser.FROM);
				this.state = 265;
				this.match(BaseRqlParser.ALL_DOCS);
				}
				break;

			case 3:
				_localctx = new CollectionByNameContext(_localctx);
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 266;
				this.fromMode();
				this.state = 267;
				(_localctx as CollectionByNameContext)._collection = this.collectionName();
				this.state = 269;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (((((_la - 26)) & ~0x1F) === 0 && ((1 << (_la - 26)) & ((1 << (BaseRqlParser.ALL - 26)) | (1 << (BaseRqlParser.ALPHANUMERIC - 26)) | (1 << (BaseRqlParser.AND - 26)) | (1 << (BaseRqlParser.AS - 26)) | (1 << (BaseRqlParser.BETWEEN - 26)) | (1 << (BaseRqlParser.DISTINCT - 26)) | (1 << (BaseRqlParser.DOUBLE - 26)) | (1 << (BaseRqlParser.ENDS_WITH - 26)) | (1 << (BaseRqlParser.STARTS_WITH - 26)) | (1 << (BaseRqlParser.FALSE - 26)) | (1 << (BaseRqlParser.FACET - 26)) | (1 << (BaseRqlParser.ID - 26)) | (1 << (BaseRqlParser.IN - 26)) | (1 << (BaseRqlParser.INTERSECT - 26)) | (1 << (BaseRqlParser.LONG - 26)) | (1 << (BaseRqlParser.MATCH - 26)) | (1 << (BaseRqlParser.METADATA - 26)) | (1 << (BaseRqlParser.MORELIKETHIS - 26)) | (1 << (BaseRqlParser.NOT - 26)) | (1 << (BaseRqlParser.NULL - 26)) | (1 << (BaseRqlParser.OR - 26)))) !== 0) || ((((_la - 60)) & ~0x1F) === 0 && ((1 << (_la - 60)) & ((1 << (BaseRqlParser.SORTING - 60)) | (1 << (BaseRqlParser.STRING_W - 60)) | (1 << (BaseRqlParser.TO - 60)) | (1 << (BaseRqlParser.TRUE - 60)) | (1 << (BaseRqlParser.WITH - 60)) | (1 << (BaseRqlParser.EXACT - 60)) | (1 << (BaseRqlParser.BOOST - 60)) | (1 << (BaseRqlParser.SEARCH - 60)) | (1 << (BaseRqlParser.FUZZY - 60)) | (1 << (BaseRqlParser.DOUBLE_QUOTE_STRING - 60)) | (1 << (BaseRqlParser.SINGLE_QUOTE_STRING - 60)) | (1 << (BaseRqlParser.WORD - 60)))) !== 0)) {
					{
					this.state = 268;
					this.aliasWithOptionalAs();
					}
				}

				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public indexName(): IndexNameContext {
		let _localctx: IndexNameContext = new IndexNameContext(this._ctx, this.state);
		this.enterRule(_localctx, 10, BaseRqlParser.RULE_indexName);
		try {
			this.state = 276;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.WORD:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 273;
				this.match(BaseRqlParser.WORD);
				}
				break;
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 274;
				this.string();
				}
				break;
			case BaseRqlParser.ALL:
			case BaseRqlParser.ALPHANUMERIC:
			case BaseRqlParser.AND:
			case BaseRqlParser.BETWEEN:
			case BaseRqlParser.DISTINCT:
			case BaseRqlParser.DOUBLE:
			case BaseRqlParser.ENDS_WITH:
			case BaseRqlParser.STARTS_WITH:
			case BaseRqlParser.FALSE:
			case BaseRqlParser.FACET:
			case BaseRqlParser.ID:
			case BaseRqlParser.IN:
			case BaseRqlParser.INTERSECT:
			case BaseRqlParser.LONG:
			case BaseRqlParser.MATCH:
			case BaseRqlParser.METADATA:
			case BaseRqlParser.MORELIKETHIS:
			case BaseRqlParser.NOT:
			case BaseRqlParser.NULL:
			case BaseRqlParser.OR:
			case BaseRqlParser.SORTING:
			case BaseRqlParser.STRING_W:
			case BaseRqlParser.TO:
			case BaseRqlParser.TRUE:
			case BaseRqlParser.WITH:
			case BaseRqlParser.EXACT:
			case BaseRqlParser.BOOST:
			case BaseRqlParser.SEARCH:
			case BaseRqlParser.FUZZY:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 275;
				this.identifiersWithoutRootKeywords();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public collectionName(): CollectionNameContext {
		let _localctx: CollectionNameContext = new CollectionNameContext(this._ctx, this.state);
		this.enterRule(_localctx, 12, BaseRqlParser.RULE_collectionName);
		try {
			this.state = 281;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.WORD:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 278;
				this.match(BaseRqlParser.WORD);
				}
				break;
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 279;
				this.string();
				}
				break;
			case BaseRqlParser.ALL:
			case BaseRqlParser.ALPHANUMERIC:
			case BaseRqlParser.AND:
			case BaseRqlParser.BETWEEN:
			case BaseRqlParser.DISTINCT:
			case BaseRqlParser.DOUBLE:
			case BaseRqlParser.ENDS_WITH:
			case BaseRqlParser.STARTS_WITH:
			case BaseRqlParser.FALSE:
			case BaseRqlParser.FACET:
			case BaseRqlParser.ID:
			case BaseRqlParser.IN:
			case BaseRqlParser.INTERSECT:
			case BaseRqlParser.LONG:
			case BaseRqlParser.MATCH:
			case BaseRqlParser.METADATA:
			case BaseRqlParser.MORELIKETHIS:
			case BaseRqlParser.NOT:
			case BaseRqlParser.NULL:
			case BaseRqlParser.OR:
			case BaseRqlParser.SORTING:
			case BaseRqlParser.STRING_W:
			case BaseRqlParser.TO:
			case BaseRqlParser.TRUE:
			case BaseRqlParser.WITH:
			case BaseRqlParser.EXACT:
			case BaseRqlParser.BOOST:
			case BaseRqlParser.SEARCH:
			case BaseRqlParser.FUZZY:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 280;
				this.identifiersWithoutRootKeywords();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public aliasWithOptionalAs(): AliasWithOptionalAsContext {
		let _localctx: AliasWithOptionalAsContext = new AliasWithOptionalAsContext(this._ctx, this.state);
		this.enterRule(_localctx, 14, BaseRqlParser.RULE_aliasWithOptionalAs);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 284;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.AS) {
				{
				this.state = 283;
				this.match(BaseRqlParser.AS);
				}
			}

			this.state = 286;
			_localctx._name = this.aliasName();
			this.state = 288;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.OP_Q) {
				{
				this.state = 287;
				this.asArray();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public groupByMode(): GroupByModeContext {
		let _localctx: GroupByModeContext = new GroupByModeContext(this._ctx, this.state);
		this.enterRule(_localctx, 16, BaseRqlParser.RULE_groupByMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 290;
			this.match(BaseRqlParser.GROUP_BY);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public groupByStatement(): GroupByStatementContext {
		let _localctx: GroupByStatementContext = new GroupByStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 18, BaseRqlParser.RULE_groupByStatement);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 292;
			this.groupByMode();
			{
			this.state = 293;
			_localctx._value = this.parameterWithOptionalAlias();
			}
			this.state = 298;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 294;
				this.match(BaseRqlParser.COMMA);
				{
				this.state = 295;
				this.parameterWithOptionalAlias();
				}
				}
				}
				this.state = 300;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public suggestGroupBy(): SuggestGroupByContext {
		let _localctx: SuggestGroupByContext = new SuggestGroupByContext(this._ctx, this.state);
		this.enterRule(_localctx, 20, BaseRqlParser.RULE_suggestGroupBy);
		try {
			this.enterOuterAlt(_localctx, 1);
			// tslint:disable-next-line:no-empty
			{
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public whereMode(): WhereModeContext {
		let _localctx: WhereModeContext = new WhereModeContext(this._ctx, this.state);
		this.enterRule(_localctx, 22, BaseRqlParser.RULE_whereMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 303;
			this.match(BaseRqlParser.WHERE);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public whereStatement(): WhereStatementContext {
		let _localctx: WhereStatementContext = new WhereStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 24, BaseRqlParser.RULE_whereStatement);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 305;
			this.whereMode();
			this.state = 306;
			this.expr(0);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}

	public expr(): ExprContext;
	public expr(_p: number): ExprContext;
	// @RuleVersion(0)
	public expr(_p?: number): ExprContext {
		if (_p === undefined) {
			_p = 0;
		}

		let _parentctx: ParserRuleContext = this._ctx;
		let _parentState: number = this.state;
		let _localctx: ExprContext = new ExprContext(this._ctx, _parentState);
		let _prevctx: ExprContext = _localctx;
		let _startState: number = 26;
		this.enterRecursionRule(_localctx, 26, BaseRqlParser.RULE_expr, _p);
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 332;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 23, this._ctx) ) {
			case 1:
				{
				_localctx = new OpParContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;

				this.state = 309;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 310;
				this.expr(0);
				this.state = 311;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;

			case 2:
				{
				_localctx = new EqualExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 313;
				(_localctx as EqualExpressionContext)._left = this.exprValue();
				this.state = 314;
				this.match(BaseRqlParser.EQUAL);
				this.state = 315;
				(_localctx as EqualExpressionContext)._right = this.exprValue();
				}
				break;

			case 3:
				{
				_localctx = new MathExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 317;
				(_localctx as MathExpressionContext)._left = this.exprValue();
				this.state = 318;
				this.match(BaseRqlParser.MATH);
				this.state = 319;
				(_localctx as MathExpressionContext)._right = this.exprValue();
				}
				break;

			case 4:
				{
				_localctx = new SpecialFunctionstContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 321;
				this.specialFunctions();
				}
				break;

			case 5:
				{
				_localctx = new InExprContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 322;
				this.inFunction();
				}
				break;

			case 6:
				{
				_localctx = new VecExprContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 323;
				this.vectorSearch();
				}
				break;

			case 7:
				{
				_localctx = new BetweenExprContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 324;
				this.betweenFunction();
				}
				break;

			case 8:
				{
				_localctx = new NormalFuncContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 325;
				(_localctx as NormalFuncContext)._funcExpr = this.function();
				}
				break;

			case 9:
				{
				_localctx = new BooleanExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 326;
				this.match(BaseRqlParser.TRUE);
				this.state = 327;
				this.match(BaseRqlParser.AND);
				this.state = 329;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 22, this._ctx) ) {
				case 1:
					{
					this.state = 328;
					this.match(BaseRqlParser.NOT);
					}
					break;
				}
				this.state = 331;
				this.expr(1);
				}
				break;
			}
			this._ctx._stop = this._input.tryLT(-1);
			this.state = 340;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 24, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = _localctx;
					{
					{
					_localctx = new BinaryExpressionContext(new ExprContext(_parentctx, _parentState));
					(_localctx as BinaryExpressionContext)._left = _prevctx;
					this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_expr);
					this.state = 334;
					if (!(this.precpred(this._ctx, 10))) {
						throw this.createFailedPredicateException("this.precpred(this._ctx, 10)");
					}
					this.state = 335;
					this.binary();
					this.state = 336;
					(_localctx as BinaryExpressionContext)._right = this.expr(11);
					}
					}
				}
				this.state = 342;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 24, this._ctx);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public binary(): BinaryContext {
		let _localctx: BinaryContext = new BinaryContext(this._ctx, this.state);
		this.enterRule(_localctx, 28, BaseRqlParser.RULE_binary);
		try {
			this.state = 349;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 25, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 343;
				this.match(BaseRqlParser.AND);
				this.state = 344;
				this.match(BaseRqlParser.NOT);
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 345;
				this.match(BaseRqlParser.OR);
				this.state = 346;
				this.match(BaseRqlParser.NOT);
				}
				break;

			case 3:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 347;
				this.match(BaseRqlParser.AND);
				}
				break;

			case 4:
				this.enterOuterAlt(_localctx, 4);
				{
				this.state = 348;
				this.match(BaseRqlParser.OR);
				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public exprValue(): ExprValueContext {
		let _localctx: ExprValueContext = new ExprValueContext(this._ctx, this.state);
		this.enterRule(_localctx, 30, BaseRqlParser.RULE_exprValue);
		try {
			_localctx = new ParameterExprContext(_localctx);
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 351;
			this.literal();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public inFunction(): InFunctionContext {
		let _localctx: InFunctionContext = new InFunctionContext(this._ctx, this.state);
		this.enterRule(_localctx, 32, BaseRqlParser.RULE_inFunction);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 353;
			_localctx._value = this.literal();
			this.state = 355;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.ALL) {
				{
				this.state = 354;
				this.match(BaseRqlParser.ALL);
				}
			}

			this.state = 357;
			this.match(BaseRqlParser.IN);
			this.state = 358;
			this.match(BaseRqlParser.OP_PAR);
			this.state = 359;
			_localctx._first = this.literal();
			this.state = 364;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 360;
				this.match(BaseRqlParser.COMMA);
				this.state = 361;
				_localctx._next = this.literal();
				}
				}
				this.state = 366;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 367;
			this.match(BaseRqlParser.CL_PAR);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public betweenFunction(): BetweenFunctionContext {
		let _localctx: BetweenFunctionContext = new BetweenFunctionContext(this._ctx, this.state);
		this.enterRule(_localctx, 34, BaseRqlParser.RULE_betweenFunction);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 369;
			_localctx._value = this.literal();
			this.state = 370;
			this.match(BaseRqlParser.BETWEEN);
			this.state = 371;
			_localctx._from = this.literal();
			this.state = 372;
			this.match(BaseRqlParser.AND);
			this.state = 373;
			_localctx._to = this.literal();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public specialFunctions(): SpecialFunctionsContext {
		let _localctx: SpecialFunctionsContext = new SpecialFunctionsContext(this._ctx, this.state);
		this.enterRule(_localctx, 36, BaseRqlParser.RULE_specialFunctions);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 375;
			this.specialFunctionName();
			this.state = 376;
			this.match(BaseRqlParser.OP_PAR);
			this.state = 391;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << BaseRqlParser.OP_PAR) | (1 << BaseRqlParser.OP_Q) | (1 << BaseRqlParser.DOL) | (1 << BaseRqlParser.ALL) | (1 << BaseRqlParser.ALPHANUMERIC) | (1 << BaseRqlParser.AND) | (1 << BaseRqlParser.BETWEEN))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (BaseRqlParser.DISTINCT - 32)) | (1 << (BaseRqlParser.DOUBLE - 32)) | (1 << (BaseRqlParser.ENDS_WITH - 32)) | (1 << (BaseRqlParser.STARTS_WITH - 32)) | (1 << (BaseRqlParser.FALSE - 32)) | (1 << (BaseRqlParser.FACET - 32)) | (1 << (BaseRqlParser.FROM - 32)) | (1 << (BaseRqlParser.GROUP_BY - 32)) | (1 << (BaseRqlParser.ID - 32)) | (1 << (BaseRqlParser.IN - 32)) | (1 << (BaseRqlParser.INCLUDE - 32)) | (1 << (BaseRqlParser.INDEX - 32)) | (1 << (BaseRqlParser.INTERSECT - 32)) | (1 << (BaseRqlParser.LOAD - 32)) | (1 << (BaseRqlParser.LONG - 32)) | (1 << (BaseRqlParser.MATCH - 32)) | (1 << (BaseRqlParser.METADATA - 32)) | (1 << (BaseRqlParser.MORELIKETHIS - 32)) | (1 << (BaseRqlParser.NOT - 32)) | (1 << (BaseRqlParser.NULL - 32)) | (1 << (BaseRqlParser.OR - 32)) | (1 << (BaseRqlParser.ORDER_BY - 32)) | (1 << (BaseRqlParser.SELECT - 32)) | (1 << (BaseRqlParser.SORTING - 32)) | (1 << (BaseRqlParser.STRING_W - 32)) | (1 << (BaseRqlParser.TO - 32)) | (1 << (BaseRqlParser.TRUE - 32)))) !== 0) || ((((_la - 64)) & ~0x1F) === 0 && ((1 << (_la - 64)) & ((1 << (BaseRqlParser.WHERE - 64)) | (1 << (BaseRqlParser.WITH - 64)) | (1 << (BaseRqlParser.EXACT - 64)) | (1 << (BaseRqlParser.BOOST - 64)) | (1 << (BaseRqlParser.SEARCH - 64)) | (1 << (BaseRqlParser.LIMIT - 64)) | (1 << (BaseRqlParser.FUZZY - 64)) | (1 << (BaseRqlParser.FILTER - 64)) | (1 << (BaseRqlParser.FILTER_LIMIT - 64)) | (1 << (BaseRqlParser.NUM - 64)) | (1 << (BaseRqlParser.DOUBLE_QUOTE_STRING - 64)) | (1 << (BaseRqlParser.SINGLE_QUOTE_STRING - 64)) | (1 << (BaseRqlParser.WORD - 64)))) !== 0)) {
				{
				this.state = 377;
				this.specialParam(0);
				this.state = 379;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === BaseRqlParser.AS) {
					{
					this.state = 378;
					this.aliasWithRequiredAs();
					}
				}

				this.state = 388;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === BaseRqlParser.COMMA) {
					{
					{
					this.state = 381;
					this.match(BaseRqlParser.COMMA);
					this.state = 382;
					this.specialParam(0);
					this.state = 384;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la === BaseRqlParser.AS) {
						{
						this.state = 383;
						this.aliasWithRequiredAs();
						}
					}

					}
					}
					this.state = 390;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				}
			}

			this.state = 393;
			this.match(BaseRqlParser.CL_PAR);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public specialFunctionName(): SpecialFunctionNameContext {
		let _localctx: SpecialFunctionNameContext = new SpecialFunctionNameContext(this._ctx, this.state);
		this.enterRule(_localctx, 38, BaseRqlParser.RULE_specialFunctionName);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 395;
			_la = this._input.LA(1);
			if (!(((((_la - 34)) & ~0x1F) === 0 && ((1 << (_la - 34)) & ((1 << (BaseRqlParser.ENDS_WITH - 34)) | (1 << (BaseRqlParser.STARTS_WITH - 34)) | (1 << (BaseRqlParser.FACET - 34)) | (1 << (BaseRqlParser.ID - 34)) | (1 << (BaseRqlParser.INTERSECT - 34)) | (1 << (BaseRqlParser.MORELIKETHIS - 34)))) !== 0) || ((((_la - 66)) & ~0x1F) === 0 && ((1 << (_la - 66)) & ((1 << (BaseRqlParser.EXACT - 66)) | (1 << (BaseRqlParser.BOOST - 66)) | (1 << (BaseRqlParser.SEARCH - 66)) | (1 << (BaseRqlParser.FUZZY - 66)))) !== 0))) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public vectorSearch(): VectorSearchContext {
		let _localctx: VectorSearchContext = new VectorSearchContext(this._ctx, this.state);
		this.enterRule(_localctx, 40, BaseRqlParser.RULE_vectorSearch);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 397;
			this.match(BaseRqlParser.VECTOR_SEARCH);
			this.state = 398;
			this.vectorSearchParam();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public vectorSearchParam(): VectorSearchParamContext {
		let _localctx: VectorSearchParamContext = new VectorSearchParamContext(this._ctx, this.state);
		this.enterRule(_localctx, 42, BaseRqlParser.RULE_vectorSearchParam);
		let _la: number;
		try {
			this.state = 431;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 36, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 400;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 407;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case BaseRqlParser.EMBEDDING_ARRAY:
					{
					{
					this.state = 401;
					this.match(BaseRqlParser.EMBEDDING_ARRAY);
					this.state = 402;
					this.match(BaseRqlParser.OP_PAR);
					this.state = 403;
					this.variable();
					this.state = 404;
					this.match(BaseRqlParser.CL_PAR);
					}
					}
					break;
				case BaseRqlParser.OP_Q:
				case BaseRqlParser.DOL:
				case BaseRqlParser.ALL:
				case BaseRqlParser.ALPHANUMERIC:
				case BaseRqlParser.AND:
				case BaseRqlParser.BETWEEN:
				case BaseRqlParser.DISTINCT:
				case BaseRqlParser.DOUBLE:
				case BaseRqlParser.ENDS_WITH:
				case BaseRqlParser.STARTS_WITH:
				case BaseRqlParser.FALSE:
				case BaseRqlParser.FACET:
				case BaseRqlParser.FROM:
				case BaseRqlParser.GROUP_BY:
				case BaseRqlParser.ID:
				case BaseRqlParser.IN:
				case BaseRqlParser.INCLUDE:
				case BaseRqlParser.INDEX:
				case BaseRqlParser.INTERSECT:
				case BaseRqlParser.LOAD:
				case BaseRqlParser.LONG:
				case BaseRqlParser.MATCH:
				case BaseRqlParser.METADATA:
				case BaseRqlParser.MORELIKETHIS:
				case BaseRqlParser.NOT:
				case BaseRqlParser.NULL:
				case BaseRqlParser.OR:
				case BaseRqlParser.ORDER_BY:
				case BaseRqlParser.SELECT:
				case BaseRqlParser.SORTING:
				case BaseRqlParser.STRING_W:
				case BaseRqlParser.TO:
				case BaseRqlParser.TRUE:
				case BaseRqlParser.WHERE:
				case BaseRqlParser.WITH:
				case BaseRqlParser.EXACT:
				case BaseRqlParser.BOOST:
				case BaseRqlParser.SEARCH:
				case BaseRqlParser.LIMIT:
				case BaseRqlParser.FUZZY:
				case BaseRqlParser.FILTER:
				case BaseRqlParser.FILTER_LIMIT:
				case BaseRqlParser.NUM:
				case BaseRqlParser.DOUBLE_QUOTE_STRING:
				case BaseRqlParser.SINGLE_QUOTE_STRING:
				case BaseRqlParser.WORD:
					{
					this.state = 406;
					this.literal();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				this.state = 409;
				this.match(BaseRqlParser.COMMA);
				this.state = 410;
				this.vectorSearchValue();
				this.state = 412;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === BaseRqlParser.COMMA) {
					{
					this.state = 411;
					this.vectorSearchAdditionalParams();
					}
				}

				this.state = 414;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 416;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 417;
				this.match(BaseRqlParser.EMBEDDING_TEXT);
				this.state = 418;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 419;
				this.literal();
				this.state = 421;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === BaseRqlParser.COMMA) {
					{
					this.state = 420;
					this.aiTaskMethod();
					}
				}

				this.state = 423;
				this.match(BaseRqlParser.CL_PAR);
				this.state = 424;
				this.match(BaseRqlParser.COMMA);
				this.state = 425;
				this.vectorSearchValue();
				this.state = 427;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === BaseRqlParser.COMMA) {
					{
					this.state = 426;
					this.vectorSearchAdditionalParams();
					}
				}

				this.state = 429;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public aiTaskMethod(): AiTaskMethodContext {
		let _localctx: AiTaskMethodContext = new AiTaskMethodContext(this._ctx, this.state);
		this.enterRule(_localctx, 44, BaseRqlParser.RULE_aiTaskMethod);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 433;
			this.match(BaseRqlParser.COMMA);
			this.state = 434;
			this.match(BaseRqlParser.AI_TASK);
			this.state = 435;
			this.match(BaseRqlParser.OP_PAR);
			this.state = 436;
			this.literal();
			this.state = 437;
			this.match(BaseRqlParser.CL_PAR);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public vectorSearchAdditionalParams(): VectorSearchAdditionalParamsContext {
		let _localctx: VectorSearchAdditionalParamsContext = new VectorSearchAdditionalParamsContext(this._ctx, this.state);
		this.enterRule(_localctx, 46, BaseRqlParser.RULE_vectorSearchAdditionalParams);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 439;
			this.match(BaseRqlParser.COMMA);
			this.state = 440;
			_la = this._input.LA(1);
			if (!(_la === BaseRqlParser.NULL || _la === BaseRqlParser.NUM)) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			this.state = 443;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.COMMA) {
				{
				this.state = 441;
				this.match(BaseRqlParser.COMMA);
				this.state = 442;
				_la = this._input.LA(1);
				if (!(_la === BaseRqlParser.NULL || _la === BaseRqlParser.NUM)) {
				this._errHandler.recoverInline(this);
				} else {
					if (this._input.LA(1) === Token.EOF) {
						this.matchedEOF = true;
					}

					this._errHandler.reportMatch(this);
					this.consume();
				}
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public vectorSearchValue(): VectorSearchValueContext {
		let _localctx: VectorSearchValueContext = new VectorSearchValueContext(this._ctx, this.state);
		this.enterRule(_localctx, 48, BaseRqlParser.RULE_vectorSearchValue);
		try {
			this.state = 451;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.OP_Q:
			case BaseRqlParser.DOL:
			case BaseRqlParser.ALL:
			case BaseRqlParser.ALPHANUMERIC:
			case BaseRqlParser.AND:
			case BaseRqlParser.BETWEEN:
			case BaseRqlParser.DISTINCT:
			case BaseRqlParser.DOUBLE:
			case BaseRqlParser.ENDS_WITH:
			case BaseRqlParser.STARTS_WITH:
			case BaseRqlParser.FALSE:
			case BaseRqlParser.FACET:
			case BaseRqlParser.FROM:
			case BaseRqlParser.GROUP_BY:
			case BaseRqlParser.ID:
			case BaseRqlParser.IN:
			case BaseRqlParser.INCLUDE:
			case BaseRqlParser.INDEX:
			case BaseRqlParser.INTERSECT:
			case BaseRqlParser.LOAD:
			case BaseRqlParser.LONG:
			case BaseRqlParser.MATCH:
			case BaseRqlParser.METADATA:
			case BaseRqlParser.MORELIKETHIS:
			case BaseRqlParser.NOT:
			case BaseRqlParser.NULL:
			case BaseRqlParser.OR:
			case BaseRqlParser.ORDER_BY:
			case BaseRqlParser.SELECT:
			case BaseRqlParser.SORTING:
			case BaseRqlParser.STRING_W:
			case BaseRqlParser.TO:
			case BaseRqlParser.TRUE:
			case BaseRqlParser.WHERE:
			case BaseRqlParser.WITH:
			case BaseRqlParser.EXACT:
			case BaseRqlParser.BOOST:
			case BaseRqlParser.SEARCH:
			case BaseRqlParser.LIMIT:
			case BaseRqlParser.FUZZY:
			case BaseRqlParser.FILTER:
			case BaseRqlParser.FILTER_LIMIT:
			case BaseRqlParser.NUM:
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
			case BaseRqlParser.WORD:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 445;
				this.literal();
				}
				break;
			case BaseRqlParser.EMBEDDING_FOR:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 446;
				this.match(BaseRqlParser.EMBEDDING_FOR);
				this.state = 447;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 448;
				this.literal();
				this.state = 449;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}

	public specialParam(): SpecialParamContext;
	public specialParam(_p: number): SpecialParamContext;
	// @RuleVersion(0)
	public specialParam(_p?: number): SpecialParamContext {
		if (_p === undefined) {
			_p = 0;
		}

		let _parentctx: ParserRuleContext = this._ctx;
		let _parentState: number = this.state;
		let _localctx: SpecialParamContext = new SpecialParamContext(this._ctx, _parentState);
		let _prevctx: SpecialParamContext = _localctx;
		let _startState: number = 50;
		this.enterRecursionRule(_localctx, 50, BaseRqlParser.RULE_specialParam, _p);
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 470;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 39, this._ctx) ) {
			case 1:
				{
				this.state = 454;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 455;
				this.specialParam(0);
				this.state = 456;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;

			case 2:
				{
				this.state = 458;
				this.variable();
				this.state = 459;
				this.match(BaseRqlParser.BETWEEN);
				this.state = 460;
				this.specialParam(12);
				}
				break;

			case 3:
				{
				this.state = 462;
				this.inFunction();
				}
				break;

			case 4:
				{
				this.state = 463;
				this.betweenFunction();
				}
				break;

			case 5:
				{
				this.state = 464;
				this.specialFunctions();
				}
				break;

			case 6:
				{
				this.state = 465;
				this.date();
				}
				break;

			case 7:
				{
				this.state = 466;
				this.function();
				}
				break;

			case 8:
				{
				this.state = 467;
				this.variable();
				}
				break;

			case 9:
				{
				this.state = 468;
				this.identifiersAllNames();
				}
				break;

			case 10:
				{
				this.state = 469;
				this.match(BaseRqlParser.NUM);
				}
				break;
			}
			this._ctx._stop = this._input.tryLT(-1);
			this.state = 486;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 41, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = _localctx;
					{
					this.state = 484;
					this._errHandler.sync(this);
					switch ( this.interpreter.adaptivePredict(this._input, 40, this._ctx) ) {
					case 1:
						{
						_localctx = new SpecialParamContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_specialParam);
						this.state = 472;
						if (!(this.precpred(this._ctx, 13))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 13)");
						}
						this.state = 473;
						this.match(BaseRqlParser.EQUAL);
						this.state = 474;
						this.specialParam(14);
						}
						break;

					case 2:
						{
						_localctx = new SpecialParamContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_specialParam);
						this.state = 475;
						if (!(this.precpred(this._ctx, 11))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 11)");
						}
						this.state = 476;
						this.match(BaseRqlParser.AND);
						this.state = 477;
						this.specialParam(12);
						}
						break;

					case 3:
						{
						_localctx = new SpecialParamContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_specialParam);
						this.state = 478;
						if (!(this.precpred(this._ctx, 10))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 10)");
						}
						this.state = 479;
						this.match(BaseRqlParser.OR);
						this.state = 480;
						this.specialParam(11);
						}
						break;

					case 4:
						{
						_localctx = new SpecialParamContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_specialParam);
						this.state = 481;
						if (!(this.precpred(this._ctx, 9))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 9)");
						}
						this.state = 482;
						this.match(BaseRqlParser.MATH);
						this.state = 483;
						this.specialParam(10);
						}
						break;
					}
					}
				}
				this.state = 488;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 41, this._ctx);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public loadMode(): LoadModeContext {
		let _localctx: LoadModeContext = new LoadModeContext(this._ctx, this.state);
		this.enterRule(_localctx, 52, BaseRqlParser.RULE_loadMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 489;
			this.match(BaseRqlParser.LOAD);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public loadStatement(): LoadStatementContext {
		let _localctx: LoadStatementContext = new LoadStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 54, BaseRqlParser.RULE_loadStatement);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 491;
			this.loadMode();
			this.state = 492;
			_localctx._item = this.loadDocumentByName();
			this.state = 497;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 493;
				this.match(BaseRqlParser.COMMA);
				this.state = 494;
				this.loadDocumentByName();
				}
				}
				this.state = 499;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public loadDocumentByName(): LoadDocumentByNameContext {
		let _localctx: LoadDocumentByNameContext = new LoadDocumentByNameContext(this._ctx, this.state);
		this.enterRule(_localctx, 56, BaseRqlParser.RULE_loadDocumentByName);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 500;
			_localctx._name = this.variable();
			this.state = 501;
			_localctx._as = this.aliasWithOptionalAs();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public orderByMode(): OrderByModeContext {
		let _localctx: OrderByModeContext = new OrderByModeContext(this._ctx, this.state);
		this.enterRule(_localctx, 58, BaseRqlParser.RULE_orderByMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 503;
			this.match(BaseRqlParser.ORDER_BY);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public orderByStatement(): OrderByStatementContext {
		let _localctx: OrderByStatementContext = new OrderByStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 60, BaseRqlParser.RULE_orderByStatement);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 505;
			this.orderByMode();
			this.state = 506;
			_localctx._value = this.orderByItem();
			this.state = 511;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 507;
				this.match(BaseRqlParser.COMMA);
				{
				this.state = 508;
				this.orderByItem();
				}
				}
				}
				this.state = 513;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public orderByItem(): OrderByItemContext {
		let _localctx: OrderByItemContext = new OrderByItemContext(this._ctx, this.state);
		this.enterRule(_localctx, 62, BaseRqlParser.RULE_orderByItem);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 514;
			_localctx._value = this.literal();
			this.state = 516;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.AS) {
				{
				this.state = 515;
				_localctx._order = this.orderBySorting();
				}
			}

			this.state = 519;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.SORTING) {
				{
				this.state = 518;
				_localctx._orderValueType = this.orderByOrder();
				}
			}

			this.state = 522;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.NULLS_FIRST || _la === BaseRqlParser.NULLS_LAST) {
				{
				this.state = 521;
				_localctx._nullsValue = this.orderByNulls();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public orderBySorting(): OrderBySortingContext {
		let _localctx: OrderBySortingContext = new OrderBySortingContext(this._ctx, this.state);
		this.enterRule(_localctx, 64, BaseRqlParser.RULE_orderBySorting);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 524;
			this.match(BaseRqlParser.AS);
			this.state = 525;
			_localctx._sortingMode = this.orderBySortingAs();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public orderBySortingAs(): OrderBySortingAsContext {
		let _localctx: OrderBySortingAsContext = new OrderBySortingAsContext(this._ctx, this.state);
		this.enterRule(_localctx, 66, BaseRqlParser.RULE_orderBySortingAs);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 527;
			_la = this._input.LA(1);
			if (!(_la === BaseRqlParser.ALPHANUMERIC || ((((_la - 33)) & ~0x1F) === 0 && ((1 << (_la - 33)) & ((1 << (BaseRqlParser.DOUBLE - 33)) | (1 << (BaseRqlParser.LONG - 33)) | (1 << (BaseRqlParser.STRING_W - 33)))) !== 0))) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public orderByOrder(): OrderByOrderContext {
		let _localctx: OrderByOrderContext = new OrderByOrderContext(this._ctx, this.state);
		this.enterRule(_localctx, 68, BaseRqlParser.RULE_orderByOrder);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 529;
			this.match(BaseRqlParser.SORTING);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public orderByNulls(): OrderByNullsContext {
		let _localctx: OrderByNullsContext = new OrderByNullsContext(this._ctx, this.state);
		this.enterRule(_localctx, 70, BaseRqlParser.RULE_orderByNulls);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 531;
			_la = this._input.LA(1);
			if (!(_la === BaseRqlParser.NULLS_FIRST || _la === BaseRqlParser.NULLS_LAST)) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public selectMode(): SelectModeContext {
		let _localctx: SelectModeContext = new SelectModeContext(this._ctx, this.state);
		this.enterRule(_localctx, 72, BaseRqlParser.RULE_selectMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 533;
			this.match(BaseRqlParser.SELECT);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public selectStatement(): SelectStatementContext {
		let _localctx: SelectStatementContext = new SelectStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 74, BaseRqlParser.RULE_selectStatement);
		let _la: number;
		try {
			this.state = 567;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 53, this._ctx) ) {
			case 1:
				_localctx = new GetAllDistinctContext(_localctx);
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 535;
				this.selectMode();
				this.state = 536;
				this.match(BaseRqlParser.DISTINCT);
				this.state = 537;
				this.match(BaseRqlParser.STAR);
				this.state = 539;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 47, this._ctx) ) {
				case 1:
					{
					this.state = 538;
					this.limitStatement();
					}
					break;
				}
				}
				break;

			case 2:
				_localctx = new ProjectIndividualFieldsContext(_localctx);
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 541;
				this.selectMode();
				this.state = 543;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 48, this._ctx) ) {
				case 1:
					{
					this.state = 542;
					this.match(BaseRqlParser.DISTINCT);
					}
					break;
				}
				this.state = 545;
				(_localctx as ProjectIndividualFieldsContext)._field = this.projectField();
				this.state = 550;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === BaseRqlParser.COMMA) {
					{
					{
					this.state = 546;
					this.match(BaseRqlParser.COMMA);
					this.state = 547;
					this.projectField();
					}
					}
					this.state = 552;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 554;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 50, this._ctx) ) {
				case 1:
					{
					this.state = 553;
					this.limitStatement();
					}
					break;
				}
				}
				break;

			case 3:
				_localctx = new JavascriptCodeContext(_localctx);
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 556;
				this.match(BaseRqlParser.JS_SELECT);
				this.state = 560;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === BaseRqlParser.JS_OP) {
					{
					{
					this.state = 557;
					this.jsBody();
					}
					}
					this.state = 562;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 563;
				this.match(BaseRqlParser.JS_CL);
				this.state = 565;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 52, this._ctx) ) {
				case 1:
					{
					this.state = 564;
					this.limitStatement();
					}
					break;
				}
				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public projectField(): ProjectFieldContext {
		let _localctx: ProjectFieldContext = new ProjectFieldContext(this._ctx, this.state);
		this.enterRule(_localctx, 76, BaseRqlParser.RULE_projectField);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 572;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 54, this._ctx) ) {
			case 1:
				{
				this.state = 569;
				this.literal();
				}
				break;

			case 2:
				{
				this.state = 570;
				this.specialFunctions();
				}
				break;

			case 3:
				{
				this.state = 571;
				this.tsProg();
				}
				break;
			}
			this.state = 575;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.AS) {
				{
				this.state = 574;
				this.aliasWithRequiredAs();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public jsFunction(): JsFunctionContext {
		let _localctx: JsFunctionContext = new JsFunctionContext(this._ctx, this.state);
		this.enterRule(_localctx, 78, BaseRqlParser.RULE_jsFunction);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 577;
			this.match(BaseRqlParser.JS_FUNCTION_DECLARATION);
			this.state = 578;
			this.match(BaseRqlParser.JFN_WORD);
			this.state = 579;
			this.match(BaseRqlParser.JFN_OP_PAR);
			this.state = 581;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.JFN_WORD) {
				{
				this.state = 580;
				this.match(BaseRqlParser.JFN_WORD);
				}
			}

			this.state = 587;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.JFN_COMMA) {
				{
				{
				this.state = 583;
				this.match(BaseRqlParser.JFN_COMMA);
				this.state = 584;
				this.match(BaseRqlParser.JFN_WORD);
				}
				}
				this.state = 589;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 590;
			this.match(BaseRqlParser.JFN_CL_PAR);
			this.state = 591;
			this.match(BaseRqlParser.JFN_OP_JS);
			this.state = 595;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.JS_OP) {
				{
				{
				this.state = 592;
				this.jsBody();
				}
				}
				this.state = 597;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 598;
			this.match(BaseRqlParser.JS_CL);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public jsBody(): JsBodyContext {
		let _localctx: JsBodyContext = new JsBodyContext(this._ctx, this.state);
		this.enterRule(_localctx, 80, BaseRqlParser.RULE_jsBody);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 600;
			this.match(BaseRqlParser.JS_OP);
			this.state = 604;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.JS_OP) {
				{
				{
				this.state = 601;
				this.jsBody();
				}
				}
				this.state = 606;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 607;
			this.match(BaseRqlParser.JS_CL);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public aliasWithRequiredAs(): AliasWithRequiredAsContext {
		let _localctx: AliasWithRequiredAsContext = new AliasWithRequiredAsContext(this._ctx, this.state);
		this.enterRule(_localctx, 82, BaseRqlParser.RULE_aliasWithRequiredAs);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 609;
			this.match(BaseRqlParser.AS);
			this.state = 610;
			_localctx._name = this.aliasName();
			this.state = 612;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.OP_Q) {
				{
				this.state = 611;
				this.asArray();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public aliasName(): AliasNameContext {
		let _localctx: AliasNameContext = new AliasNameContext(this._ctx, this.state);
		this.enterRule(_localctx, 84, BaseRqlParser.RULE_aliasName);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 617;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.WORD:
				{
				this.state = 614;
				this.match(BaseRqlParser.WORD);
				}
				break;
			case BaseRqlParser.ALL:
			case BaseRqlParser.ALPHANUMERIC:
			case BaseRqlParser.AND:
			case BaseRqlParser.BETWEEN:
			case BaseRqlParser.DISTINCT:
			case BaseRqlParser.DOUBLE:
			case BaseRqlParser.ENDS_WITH:
			case BaseRqlParser.STARTS_WITH:
			case BaseRqlParser.FALSE:
			case BaseRqlParser.FACET:
			case BaseRqlParser.ID:
			case BaseRqlParser.IN:
			case BaseRqlParser.INTERSECT:
			case BaseRqlParser.LONG:
			case BaseRqlParser.MATCH:
			case BaseRqlParser.METADATA:
			case BaseRqlParser.MORELIKETHIS:
			case BaseRqlParser.NOT:
			case BaseRqlParser.NULL:
			case BaseRqlParser.OR:
			case BaseRqlParser.SORTING:
			case BaseRqlParser.STRING_W:
			case BaseRqlParser.TO:
			case BaseRqlParser.TRUE:
			case BaseRqlParser.WITH:
			case BaseRqlParser.EXACT:
			case BaseRqlParser.BOOST:
			case BaseRqlParser.SEARCH:
			case BaseRqlParser.FUZZY:
				{
				this.state = 615;
				this.identifiersWithoutRootKeywords();
				}
				break;
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
				{
				this.state = 616;
				this.string();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public prealias(): PrealiasContext {
		let _localctx: PrealiasContext = new PrealiasContext(this._ctx, this.state);
		this.enterRule(_localctx, 86, BaseRqlParser.RULE_prealias);
		let _la: number;
		try {
			this.state = 633;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.METADATA:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 619;
				this.match(BaseRqlParser.METADATA);
				this.state = 620;
				this.match(BaseRqlParser.DOT);
				}
				break;
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
			case BaseRqlParser.WORD:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 629;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				do {
					{
					{
					this.state = 623;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case BaseRqlParser.WORD:
						{
						this.state = 621;
						this.match(BaseRqlParser.WORD);
						}
						break;
					case BaseRqlParser.DOUBLE_QUOTE_STRING:
					case BaseRqlParser.SINGLE_QUOTE_STRING:
						{
						this.state = 622;
						this.string();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					this.state = 626;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la === BaseRqlParser.OP_Q) {
						{
						this.state = 625;
						this.asArray();
						}
					}

					this.state = 628;
					this.match(BaseRqlParser.DOT);
					}
					}
					this.state = 631;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				} while (((((_la - 82)) & ~0x1F) === 0 && ((1 << (_la - 82)) & ((1 << (BaseRqlParser.DOUBLE_QUOTE_STRING - 82)) | (1 << (BaseRqlParser.SINGLE_QUOTE_STRING - 82)) | (1 << (BaseRqlParser.WORD - 82)))) !== 0));
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public asArray(): AsArrayContext {
		let _localctx: AsArrayContext = new AsArrayContext(this._ctx, this.state);
		this.enterRule(_localctx, 88, BaseRqlParser.RULE_asArray);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 635;
			this.match(BaseRqlParser.OP_Q);
			this.state = 636;
			this.match(BaseRqlParser.CL_Q);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public includeMode(): IncludeModeContext {
		let _localctx: IncludeModeContext = new IncludeModeContext(this._ctx, this.state);
		this.enterRule(_localctx, 90, BaseRqlParser.RULE_includeMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 638;
			this.match(BaseRqlParser.INCLUDE);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public includeStatement(): IncludeStatementContext {
		let _localctx: IncludeStatementContext = new IncludeStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 92, BaseRqlParser.RULE_includeStatement);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 640;
			this.includeMode();
			this.state = 643;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TIMESERIES:
				{
				this.state = 641;
				this.tsIncludeTimeseriesFunction();
				}
				break;
			case BaseRqlParser.OP_Q:
			case BaseRqlParser.DOL:
			case BaseRqlParser.ALL:
			case BaseRqlParser.ALPHANUMERIC:
			case BaseRqlParser.AND:
			case BaseRqlParser.BETWEEN:
			case BaseRqlParser.DISTINCT:
			case BaseRqlParser.DOUBLE:
			case BaseRqlParser.ENDS_WITH:
			case BaseRqlParser.STARTS_WITH:
			case BaseRqlParser.FALSE:
			case BaseRqlParser.FACET:
			case BaseRqlParser.FROM:
			case BaseRqlParser.GROUP_BY:
			case BaseRqlParser.ID:
			case BaseRqlParser.IN:
			case BaseRqlParser.INCLUDE:
			case BaseRqlParser.INDEX:
			case BaseRqlParser.INTERSECT:
			case BaseRqlParser.LOAD:
			case BaseRqlParser.LONG:
			case BaseRqlParser.MATCH:
			case BaseRqlParser.METADATA:
			case BaseRqlParser.MORELIKETHIS:
			case BaseRqlParser.NOT:
			case BaseRqlParser.NULL:
			case BaseRqlParser.OR:
			case BaseRqlParser.ORDER_BY:
			case BaseRqlParser.SELECT:
			case BaseRqlParser.SORTING:
			case BaseRqlParser.STRING_W:
			case BaseRqlParser.TO:
			case BaseRqlParser.TRUE:
			case BaseRqlParser.WHERE:
			case BaseRqlParser.WITH:
			case BaseRqlParser.EXACT:
			case BaseRqlParser.BOOST:
			case BaseRqlParser.SEARCH:
			case BaseRqlParser.LIMIT:
			case BaseRqlParser.FUZZY:
			case BaseRqlParser.FILTER:
			case BaseRqlParser.FILTER_LIMIT:
			case BaseRqlParser.NUM:
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
			case BaseRqlParser.WORD:
				{
				this.state = 642;
				this.literal();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 652;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 645;
				this.match(BaseRqlParser.COMMA);
				this.state = 648;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case BaseRqlParser.OP_Q:
				case BaseRqlParser.DOL:
				case BaseRqlParser.ALL:
				case BaseRqlParser.ALPHANUMERIC:
				case BaseRqlParser.AND:
				case BaseRqlParser.BETWEEN:
				case BaseRqlParser.DISTINCT:
				case BaseRqlParser.DOUBLE:
				case BaseRqlParser.ENDS_WITH:
				case BaseRqlParser.STARTS_WITH:
				case BaseRqlParser.FALSE:
				case BaseRqlParser.FACET:
				case BaseRqlParser.FROM:
				case BaseRqlParser.GROUP_BY:
				case BaseRqlParser.ID:
				case BaseRqlParser.IN:
				case BaseRqlParser.INCLUDE:
				case BaseRqlParser.INDEX:
				case BaseRqlParser.INTERSECT:
				case BaseRqlParser.LOAD:
				case BaseRqlParser.LONG:
				case BaseRqlParser.MATCH:
				case BaseRqlParser.METADATA:
				case BaseRqlParser.MORELIKETHIS:
				case BaseRqlParser.NOT:
				case BaseRqlParser.NULL:
				case BaseRqlParser.OR:
				case BaseRqlParser.ORDER_BY:
				case BaseRqlParser.SELECT:
				case BaseRqlParser.SORTING:
				case BaseRqlParser.STRING_W:
				case BaseRqlParser.TO:
				case BaseRqlParser.TRUE:
				case BaseRqlParser.WHERE:
				case BaseRqlParser.WITH:
				case BaseRqlParser.EXACT:
				case BaseRqlParser.BOOST:
				case BaseRqlParser.SEARCH:
				case BaseRqlParser.LIMIT:
				case BaseRqlParser.FUZZY:
				case BaseRqlParser.FILTER:
				case BaseRqlParser.FILTER_LIMIT:
				case BaseRqlParser.NUM:
				case BaseRqlParser.DOUBLE_QUOTE_STRING:
				case BaseRqlParser.SINGLE_QUOTE_STRING:
				case BaseRqlParser.WORD:
					{
					this.state = 646;
					this.literal();
					}
					break;
				case BaseRqlParser.TIMESERIES:
					{
					this.state = 647;
					this.tsIncludeTimeseriesFunction();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				this.state = 654;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public limitStatement(): LimitStatementContext {
		let _localctx: LimitStatementContext = new LimitStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 94, BaseRqlParser.RULE_limitStatement);
		let _la: number;
		try {
			this.state = 667;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.LIMIT:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 655;
				this.match(BaseRqlParser.LIMIT);
				this.state = 656;
				this.variable();
				this.state = 659;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === BaseRqlParser.COMMA || _la === BaseRqlParser.OFFSET) {
					{
					this.state = 657;
					_la = this._input.LA(1);
					if (!(_la === BaseRqlParser.COMMA || _la === BaseRqlParser.OFFSET)) {
					this._errHandler.recoverInline(this);
					} else {
						if (this._input.LA(1) === Token.EOF) {
							this.matchedEOF = true;
						}

						this._errHandler.reportMatch(this);
						this.consume();
					}
					this.state = 658;
					this.variable();
					}
				}

				this.state = 663;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 70, this._ctx) ) {
				case 1:
					{
					this.state = 661;
					this.match(BaseRqlParser.FILTER_LIMIT);
					this.state = 662;
					this.variable();
					}
					break;
				}
				}
				break;
			case BaseRqlParser.FILTER_LIMIT:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 665;
				this.match(BaseRqlParser.FILTER_LIMIT);
				this.state = 666;
				this.variable();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public variable(): VariableContext {
		let _localctx: VariableContext = new VariableContext(this._ctx, this.state);
		this.enterRule(_localctx, 96, BaseRqlParser.RULE_variable);
		try {
			this.state = 674;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 72, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 669;
				_localctx._name = this.memberName();
				this.state = 670;
				this.match(BaseRqlParser.DOT);
				this.state = 671;
				_localctx._member = this.variable();
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 673;
				_localctx._name = this.memberName();
				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public memberName(): MemberNameContext {
		let _localctx: MemberNameContext = new MemberNameContext(this._ctx, this.state);
		this.enterRule(_localctx, 98, BaseRqlParser.RULE_memberName);
		try {
			this.state = 678;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.DOL:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 676;
				this.cacheParam();
				}
				break;
			case BaseRqlParser.OP_Q:
			case BaseRqlParser.ALL:
			case BaseRqlParser.ALPHANUMERIC:
			case BaseRqlParser.AND:
			case BaseRqlParser.BETWEEN:
			case BaseRqlParser.DISTINCT:
			case BaseRqlParser.DOUBLE:
			case BaseRqlParser.ENDS_WITH:
			case BaseRqlParser.STARTS_WITH:
			case BaseRqlParser.FALSE:
			case BaseRqlParser.FACET:
			case BaseRqlParser.FROM:
			case BaseRqlParser.GROUP_BY:
			case BaseRqlParser.ID:
			case BaseRqlParser.IN:
			case BaseRqlParser.INCLUDE:
			case BaseRqlParser.INDEX:
			case BaseRqlParser.INTERSECT:
			case BaseRqlParser.LOAD:
			case BaseRqlParser.LONG:
			case BaseRqlParser.MATCH:
			case BaseRqlParser.METADATA:
			case BaseRqlParser.MORELIKETHIS:
			case BaseRqlParser.NOT:
			case BaseRqlParser.NULL:
			case BaseRqlParser.OR:
			case BaseRqlParser.ORDER_BY:
			case BaseRqlParser.SELECT:
			case BaseRqlParser.SORTING:
			case BaseRqlParser.STRING_W:
			case BaseRqlParser.TO:
			case BaseRqlParser.TRUE:
			case BaseRqlParser.WHERE:
			case BaseRqlParser.WITH:
			case BaseRqlParser.EXACT:
			case BaseRqlParser.BOOST:
			case BaseRqlParser.SEARCH:
			case BaseRqlParser.LIMIT:
			case BaseRqlParser.FUZZY:
			case BaseRqlParser.FILTER:
			case BaseRqlParser.FILTER_LIMIT:
			case BaseRqlParser.NUM:
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
			case BaseRqlParser.WORD:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 677;
				this.param();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public param(): ParamContext {
		let _localctx: ParamContext = new ParamContext(this._ctx, this.state);
		this.enterRule(_localctx, 100, BaseRqlParser.RULE_param);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 688;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 74, this._ctx) ) {
			case 1:
				{
				this.state = 680;
				this.match(BaseRqlParser.NUM);
				}
				break;

			case 2:
				{
				this.state = 681;
				this.match(BaseRqlParser.WORD);
				}
				break;

			case 3:
				{
				this.state = 682;
				this.date();
				}
				break;

			case 4:
				{
				this.state = 683;
				this.string();
				}
				break;

			case 5:
				{
				this.state = 684;
				this.match(BaseRqlParser.ID);
				this.state = 685;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 686;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;

			case 6:
				{
				this.state = 687;
				this.identifiersAllNames();
				}
				break;
			}
			this.state = 691;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 75, this._ctx) ) {
			case 1:
				{
				this.state = 690;
				this.asArray();
				}
				break;
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public literal(): LiteralContext {
		let _localctx: LiteralContext = new LiteralContext(this._ctx, this.state);
		this.enterRule(_localctx, 102, BaseRqlParser.RULE_literal);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 694;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 76, this._ctx) ) {
			case 1:
				{
				this.state = 693;
				this.match(BaseRqlParser.DOL);
				}
				break;
			}
			this.state = 698;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 77, this._ctx) ) {
			case 1:
				{
				this.state = 696;
				this.function();
				}
				break;

			case 2:
				{
				this.state = 697;
				this.variable();
				}
				break;
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public cacheParam(): CacheParamContext {
		let _localctx: CacheParamContext = new CacheParamContext(this._ctx, this.state);
		this.enterRule(_localctx, 104, BaseRqlParser.RULE_cacheParam);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 700;
			this.match(BaseRqlParser.DOL);
			this.state = 701;
			this.match(BaseRqlParser.WORD);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public parameterWithOptionalAlias(): ParameterWithOptionalAliasContext {
		let _localctx: ParameterWithOptionalAliasContext = new ParameterWithOptionalAliasContext(this._ctx, this.state);
		this.enterRule(_localctx, 106, BaseRqlParser.RULE_parameterWithOptionalAlias);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 703;
			_localctx._value = this.variableOrFunction();
			this.state = 705;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.AS) {
				{
				this.state = 704;
				_localctx._as = this.aliasWithRequiredAs();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public variableOrFunction(): VariableOrFunctionContext {
		let _localctx: VariableOrFunctionContext = new VariableOrFunctionContext(this._ctx, this.state);
		this.enterRule(_localctx, 108, BaseRqlParser.RULE_variableOrFunction);
		try {
			this.state = 709;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 79, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 707;
				this.variable();
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 708;
				this.function();
				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public function(): FunctionContext {
		let _localctx: FunctionContext = new FunctionContext(this._ctx, this.state);
		this.enterRule(_localctx, 110, BaseRqlParser.RULE_function);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 711;
			_localctx._addr = this.variable();
			this.state = 712;
			this.match(BaseRqlParser.OP_PAR);
			this.state = 714;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << BaseRqlParser.OP_Q) | (1 << BaseRqlParser.DOL) | (1 << BaseRqlParser.ALL) | (1 << BaseRqlParser.ALPHANUMERIC) | (1 << BaseRqlParser.AND) | (1 << BaseRqlParser.BETWEEN))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (BaseRqlParser.DISTINCT - 32)) | (1 << (BaseRqlParser.DOUBLE - 32)) | (1 << (BaseRqlParser.ENDS_WITH - 32)) | (1 << (BaseRqlParser.STARTS_WITH - 32)) | (1 << (BaseRqlParser.FALSE - 32)) | (1 << (BaseRqlParser.FACET - 32)) | (1 << (BaseRqlParser.FROM - 32)) | (1 << (BaseRqlParser.GROUP_BY - 32)) | (1 << (BaseRqlParser.ID - 32)) | (1 << (BaseRqlParser.IN - 32)) | (1 << (BaseRqlParser.INCLUDE - 32)) | (1 << (BaseRqlParser.INDEX - 32)) | (1 << (BaseRqlParser.INTERSECT - 32)) | (1 << (BaseRqlParser.LOAD - 32)) | (1 << (BaseRqlParser.LONG - 32)) | (1 << (BaseRqlParser.MATCH - 32)) | (1 << (BaseRqlParser.METADATA - 32)) | (1 << (BaseRqlParser.MORELIKETHIS - 32)) | (1 << (BaseRqlParser.NOT - 32)) | (1 << (BaseRqlParser.NULL - 32)) | (1 << (BaseRqlParser.OR - 32)) | (1 << (BaseRqlParser.ORDER_BY - 32)) | (1 << (BaseRqlParser.SELECT - 32)) | (1 << (BaseRqlParser.SORTING - 32)) | (1 << (BaseRqlParser.STRING_W - 32)) | (1 << (BaseRqlParser.TO - 32)) | (1 << (BaseRqlParser.TRUE - 32)))) !== 0) || ((((_la - 64)) & ~0x1F) === 0 && ((1 << (_la - 64)) & ((1 << (BaseRqlParser.WHERE - 64)) | (1 << (BaseRqlParser.WITH - 64)) | (1 << (BaseRqlParser.EXACT - 64)) | (1 << (BaseRqlParser.BOOST - 64)) | (1 << (BaseRqlParser.SEARCH - 64)) | (1 << (BaseRqlParser.LIMIT - 64)) | (1 << (BaseRqlParser.FUZZY - 64)) | (1 << (BaseRqlParser.FILTER - 64)) | (1 << (BaseRqlParser.FILTER_LIMIT - 64)) | (1 << (BaseRqlParser.NUM - 64)) | (1 << (BaseRqlParser.DOUBLE_QUOTE_STRING - 64)) | (1 << (BaseRqlParser.SINGLE_QUOTE_STRING - 64)) | (1 << (BaseRqlParser.WORD - 64)))) !== 0)) {
				{
				this.state = 713;
				_localctx._args = this.arguments();
				}
			}

			this.state = 716;
			this.match(BaseRqlParser.CL_PAR);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public arguments(): ArgumentsContext {
		let _localctx: ArgumentsContext = new ArgumentsContext(this._ctx, this.state);
		this.enterRule(_localctx, 112, BaseRqlParser.RULE_arguments);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 718;
			this.literal();
			this.state = 723;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 719;
				this.match(BaseRqlParser.COMMA);
				this.state = 720;
				this.literal();
				}
				}
				this.state = 725;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public identifiersWithoutRootKeywords(): IdentifiersWithoutRootKeywordsContext {
		let _localctx: IdentifiersWithoutRootKeywordsContext = new IdentifiersWithoutRootKeywordsContext(this._ctx, this.state);
		this.enterRule(_localctx, 114, BaseRqlParser.RULE_identifiersWithoutRootKeywords);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 726;
			_la = this._input.LA(1);
			if (!(((((_la - 26)) & ~0x1F) === 0 && ((1 << (_la - 26)) & ((1 << (BaseRqlParser.ALL - 26)) | (1 << (BaseRqlParser.ALPHANUMERIC - 26)) | (1 << (BaseRqlParser.AND - 26)) | (1 << (BaseRqlParser.BETWEEN - 26)) | (1 << (BaseRqlParser.DISTINCT - 26)) | (1 << (BaseRqlParser.DOUBLE - 26)) | (1 << (BaseRqlParser.ENDS_WITH - 26)) | (1 << (BaseRqlParser.STARTS_WITH - 26)) | (1 << (BaseRqlParser.FALSE - 26)) | (1 << (BaseRqlParser.FACET - 26)) | (1 << (BaseRqlParser.ID - 26)) | (1 << (BaseRqlParser.IN - 26)) | (1 << (BaseRqlParser.INTERSECT - 26)) | (1 << (BaseRqlParser.LONG - 26)) | (1 << (BaseRqlParser.MATCH - 26)) | (1 << (BaseRqlParser.METADATA - 26)) | (1 << (BaseRqlParser.MORELIKETHIS - 26)) | (1 << (BaseRqlParser.NOT - 26)) | (1 << (BaseRqlParser.NULL - 26)) | (1 << (BaseRqlParser.OR - 26)))) !== 0) || ((((_la - 60)) & ~0x1F) === 0 && ((1 << (_la - 60)) & ((1 << (BaseRqlParser.SORTING - 60)) | (1 << (BaseRqlParser.STRING_W - 60)) | (1 << (BaseRqlParser.TO - 60)) | (1 << (BaseRqlParser.TRUE - 60)) | (1 << (BaseRqlParser.WITH - 60)) | (1 << (BaseRqlParser.EXACT - 60)) | (1 << (BaseRqlParser.BOOST - 60)) | (1 << (BaseRqlParser.SEARCH - 60)) | (1 << (BaseRqlParser.FUZZY - 60)))) !== 0))) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public rootKeywords(): RootKeywordsContext {
		let _localctx: RootKeywordsContext = new RootKeywordsContext(this._ctx, this.state);
		this.enterRule(_localctx, 116, BaseRqlParser.RULE_rootKeywords);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 728;
			_la = this._input.LA(1);
			if (!(((((_la - 38)) & ~0x1F) === 0 && ((1 << (_la - 38)) & ((1 << (BaseRqlParser.FROM - 38)) | (1 << (BaseRqlParser.GROUP_BY - 38)) | (1 << (BaseRqlParser.INCLUDE - 38)) | (1 << (BaseRqlParser.INDEX - 38)) | (1 << (BaseRqlParser.LOAD - 38)) | (1 << (BaseRqlParser.ORDER_BY - 38)) | (1 << (BaseRqlParser.SELECT - 38)) | (1 << (BaseRqlParser.WHERE - 38)))) !== 0) || ((((_la - 74)) & ~0x1F) === 0 && ((1 << (_la - 74)) & ((1 << (BaseRqlParser.LIMIT - 74)) | (1 << (BaseRqlParser.FILTER - 74)) | (1 << (BaseRqlParser.FILTER_LIMIT - 74)))) !== 0))) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public identifiersAllNames(): IdentifiersAllNamesContext {
		let _localctx: IdentifiersAllNamesContext = new IdentifiersAllNamesContext(this._ctx, this.state);
		this.enterRule(_localctx, 118, BaseRqlParser.RULE_identifiersAllNames);
		try {
			this.state = 732;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.ALL:
			case BaseRqlParser.ALPHANUMERIC:
			case BaseRqlParser.AND:
			case BaseRqlParser.BETWEEN:
			case BaseRqlParser.DISTINCT:
			case BaseRqlParser.DOUBLE:
			case BaseRqlParser.ENDS_WITH:
			case BaseRqlParser.STARTS_WITH:
			case BaseRqlParser.FALSE:
			case BaseRqlParser.FACET:
			case BaseRqlParser.ID:
			case BaseRqlParser.IN:
			case BaseRqlParser.INTERSECT:
			case BaseRqlParser.LONG:
			case BaseRqlParser.MATCH:
			case BaseRqlParser.METADATA:
			case BaseRqlParser.MORELIKETHIS:
			case BaseRqlParser.NOT:
			case BaseRqlParser.NULL:
			case BaseRqlParser.OR:
			case BaseRqlParser.SORTING:
			case BaseRqlParser.STRING_W:
			case BaseRqlParser.TO:
			case BaseRqlParser.TRUE:
			case BaseRqlParser.WITH:
			case BaseRqlParser.EXACT:
			case BaseRqlParser.BOOST:
			case BaseRqlParser.SEARCH:
			case BaseRqlParser.FUZZY:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 730;
				this.identifiersWithoutRootKeywords();
				}
				break;
			case BaseRqlParser.FROM:
			case BaseRqlParser.GROUP_BY:
			case BaseRqlParser.INCLUDE:
			case BaseRqlParser.INDEX:
			case BaseRqlParser.LOAD:
			case BaseRqlParser.ORDER_BY:
			case BaseRqlParser.SELECT:
			case BaseRqlParser.WHERE:
			case BaseRqlParser.LIMIT:
			case BaseRqlParser.FILTER:
			case BaseRqlParser.FILTER_LIMIT:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 731;
				this.rootKeywords();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public date(): DateContext {
		let _localctx: DateContext = new DateContext(this._ctx, this.state);
		this.enterRule(_localctx, 120, BaseRqlParser.RULE_date);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 734;
			this.match(BaseRqlParser.OP_Q);
			this.state = 737;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.NULL:
				{
				this.state = 735;
				this.match(BaseRqlParser.NULL);
				}
				break;
			case BaseRqlParser.WORD:
				{
				this.state = 736;
				this.dateString();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 739;
			this.match(BaseRqlParser.TO);
			this.state = 742;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.NULL:
				{
				this.state = 740;
				this.match(BaseRqlParser.NULL);
				}
				break;
			case BaseRqlParser.WORD:
				{
				this.state = 741;
				this.dateString();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 744;
			this.match(BaseRqlParser.CL_Q);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public dateString(): DateStringContext {
		let _localctx: DateStringContext = new DateStringContext(this._ctx, this.state);
		this.enterRule(_localctx, 122, BaseRqlParser.RULE_dateString);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 746;
			this.match(BaseRqlParser.WORD);
			this.state = 747;
			this.match(BaseRqlParser.DOT);
			this.state = 748;
			this.match(BaseRqlParser.NUM);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsProg(): TsProgContext {
		let _localctx: TsProgContext = new TsProgContext(this._ctx, this.state);
		this.enterRule(_localctx, 124, BaseRqlParser.RULE_tsProg);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 750;
			this.match(BaseRqlParser.TIMESERIES);
			this.state = 751;
			this.tsQueryBody();
			this.state = 752;
			this.match(BaseRqlParser.TS_CL_PAR);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsIncludeTimeseriesFunction(): TsIncludeTimeseriesFunctionContext {
		let _localctx: TsIncludeTimeseriesFunctionContext = new TsIncludeTimeseriesFunctionContext(this._ctx, this.state);
		this.enterRule(_localctx, 126, BaseRqlParser.RULE_tsIncludeTimeseriesFunction);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 754;
			this.match(BaseRqlParser.TIMESERIES);
			this.state = 755;
			this.tsLiteral();
			this.state = 758;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 85, this._ctx) ) {
			case 1:
				{
				this.state = 756;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 757;
				this.tsIncludeLiteral();
				}
				break;
			}
			this.state = 762;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_COMMA) {
				{
				this.state = 760;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 761;
				this.tsIncludeLiteral();
				}
			}

			this.state = 764;
			this.match(BaseRqlParser.TS_CL_PAR);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsIncludeLiteral(): TsIncludeLiteralContext {
		let _localctx: TsIncludeLiteralContext = new TsIncludeLiteralContext(this._ctx, this.state);
		this.enterRule(_localctx, 128, BaseRqlParser.RULE_tsIncludeLiteral);
		try {
			this.state = 768;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TS_DOL:
			case BaseRqlParser.TS_STRING:
			case BaseRqlParser.TS_WORD:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 766;
				this.tsLiteral();
				}
				break;
			case BaseRqlParser.TS_FIRST:
			case BaseRqlParser.TS_LAST:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 767;
				this.tsIncludeSpecialMethod();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsIncludeSpecialMethod(): TsIncludeSpecialMethodContext {
		let _localctx: TsIncludeSpecialMethodContext = new TsIncludeSpecialMethodContext(this._ctx, this.state);
		this.enterRule(_localctx, 130, BaseRqlParser.RULE_tsIncludeSpecialMethod);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 770;
			_la = this._input.LA(1);
			if (!(_la === BaseRqlParser.TS_FIRST || _la === BaseRqlParser.TS_LAST)) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			this.state = 771;
			this.match(BaseRqlParser.TS_OP_PAR);
			this.state = 772;
			this.match(BaseRqlParser.TS_NUM);
			this.state = 775;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_COMMA) {
				{
				this.state = 773;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 774;
				this.match(BaseRqlParser.TS_STRING);
				}
			}

			this.state = 777;
			this.match(BaseRqlParser.TS_CL_PAR);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsQueryBody(): TsQueryBodyContext {
		let _localctx: TsQueryBodyContext = new TsQueryBodyContext(this._ctx, this.state);
		this.enterRule(_localctx, 132, BaseRqlParser.RULE_tsQueryBody);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 779;
			_localctx._from = this.tsFROM();
			this.state = 781;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 105)) & ~0x1F) === 0 && ((1 << (_la - 105)) & ((1 << (BaseRqlParser.TS_BETWEEN - 105)) | (1 << (BaseRqlParser.TS_FIRST - 105)) | (1 << (BaseRqlParser.TS_LAST - 105)))) !== 0)) {
				{
				this.state = 780;
				_localctx._range = this.tsTimeRangeStatement();
				}
			}

			this.state = 784;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_LOAD) {
				{
				this.state = 783;
				_localctx._load = this.tsLoadStatement();
				}
			}

			this.state = 787;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_WHERE) {
				{
				this.state = 786;
				_localctx._where = this.tsWHERE();
				}
			}

			this.state = 790;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_GROUPBY) {
				{
				this.state = 789;
				_localctx._groupBy = this.tsGroupBy();
				}
			}

			this.state = 793;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_SELECT) {
				{
				this.state = 792;
				_localctx._select = this.tsSelect();
				}
			}

			this.state = 796;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_SCALE) {
				{
				this.state = 795;
				_localctx._scale = this.tsSelectScaleProjection();
				}
			}

			this.state = 799;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_OFFSET) {
				{
				this.state = 798;
				_localctx._offset = this.tsOffset();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsOffset(): TsOffsetContext {
		let _localctx: TsOffsetContext = new TsOffsetContext(this._ctx, this.state);
		this.enterRule(_localctx, 134, BaseRqlParser.RULE_tsOffset);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 801;
			this.match(BaseRqlParser.TS_OFFSET);
			this.state = 802;
			this.match(BaseRqlParser.TS_STRING);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsFunction(): TsFunctionContext {
		let _localctx: TsFunctionContext = new TsFunctionContext(this._ctx, this.state);
		this.enterRule(_localctx, 136, BaseRqlParser.RULE_tsFunction);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 804;
			this.match(BaseRqlParser.TIMESERIES_FUNCTION_DECLARATION);
			this.state = 805;
			this.match(BaseRqlParser.TS_WORD);
			this.state = 806;
			this.match(BaseRqlParser.TS_OP_PAR);
			this.state = 807;
			this.match(BaseRqlParser.TS_WORD);
			this.state = 812;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.TS_COMMA) {
				{
				{
				this.state = 808;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 809;
				this.match(BaseRqlParser.TS_WORD);
				}
				}
				this.state = 814;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 815;
			this.match(BaseRqlParser.TS_CL_PAR);
			this.state = 816;
			this.match(BaseRqlParser.TS_OP_C);
			this.state = 817;
			_localctx._body = this.tsQueryBody();
			this.state = 818;
			this.match(BaseRqlParser.TS_CL_C);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsTimeRangeStatement(): TsTimeRangeStatementContext {
		let _localctx: TsTimeRangeStatementContext = new TsTimeRangeStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 138, BaseRqlParser.RULE_tsTimeRangeStatement);
		try {
			this.state = 826;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 97, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 820;
				this.tsBetween();
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 821;
				_localctx._first = this.tsTimeRangeFirst();
				this.state = 822;
				_localctx._last = this.tsTimeRangeLast();
				}
				break;

			case 3:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 824;
				_localctx._first = this.tsTimeRangeFirst();
				}
				break;

			case 4:
				this.enterOuterAlt(_localctx, 4);
				{
				this.state = 825;
				_localctx._last = this.tsTimeRangeLast();
				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsLoadStatement(): TsLoadStatementContext {
		let _localctx: TsLoadStatementContext = new TsLoadStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 140, BaseRqlParser.RULE_tsLoadStatement);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 828;
			this.match(BaseRqlParser.TS_LOAD);
			this.state = 829;
			this.tsAlias();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsAlias(): TsAliasContext {
		let _localctx: TsAliasContext = new TsAliasContext(this._ctx, this.state);
		this.enterRule(_localctx, 142, BaseRqlParser.RULE_tsAlias);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 831;
			this.match(BaseRqlParser.TS_AS);
			this.state = 832;
			_localctx._alias_text = this.tsLiteral();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsFROM(): TsFROMContext {
		let _localctx: TsFROMContext = new TsFROMContext(this._ctx, this.state);
		this.enterRule(_localctx, 144, BaseRqlParser.RULE_tsFROM);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 834;
			this.match(BaseRqlParser.TS_FROM);
			this.state = 835;
			_localctx._name = this.tsCollectionName();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsWHERE(): TsWHEREContext {
		let _localctx: TsWHEREContext = new TsWHEREContext(this._ctx, this.state);
		this.enterRule(_localctx, 146, BaseRqlParser.RULE_tsWHERE);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 837;
			this.match(BaseRqlParser.TS_WHERE);
			this.state = 838;
			this.tsExpr(0);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}

	public tsExpr(): TsExprContext;
	public tsExpr(_p: number): TsExprContext;
	// @RuleVersion(0)
	public tsExpr(_p?: number): TsExprContext {
		if (_p === undefined) {
			_p = 0;
		}

		let _parentctx: ParserRuleContext = this._ctx;
		let _parentState: number = this.state;
		let _localctx: TsExprContext = new TsExprContext(this._ctx, _parentState);
		let _prevctx: TsExprContext = _localctx;
		let _startState: number = 148;
		this.enterRecursionRule(_localctx, 148, BaseRqlParser.RULE_tsExpr, _p);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 852;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TS_OP_PAR:
				{
				_localctx = new TsOpParContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;

				this.state = 841;
				this.match(BaseRqlParser.TS_OP_PAR);
				this.state = 842;
				this.tsExpr(0);
				this.state = 843;
				this.match(BaseRqlParser.TS_CL_PAR);
				}
				break;
			case BaseRqlParser.TS_TRUE:
				{
				_localctx = new TsBooleanExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 845;
				this.match(BaseRqlParser.TS_TRUE);
				this.state = 846;
				this.match(BaseRqlParser.TS_AND);
				this.state = 848;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === BaseRqlParser.TS_NOT) {
					{
					this.state = 847;
					this.match(BaseRqlParser.TS_NOT);
					}
				}

				this.state = 850;
				this.tsExpr(2);
				}
				break;
			case BaseRqlParser.TS_DOL:
			case BaseRqlParser.TS_STRING:
			case BaseRqlParser.TS_WORD:
				{
				_localctx = new TsLiteralExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 851;
				this.tsLiteral();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this._ctx._stop = this._input.tryLT(-1);
			this.state = 863;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 101, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = _localctx;
					{
					this.state = 861;
					this._errHandler.sync(this);
					switch ( this.interpreter.adaptivePredict(this._input, 100, this._ctx) ) {
					case 1:
						{
						_localctx = new TsMathExpressionContext(new TsExprContext(_parentctx, _parentState));
						(_localctx as TsMathExpressionContext)._left = _prevctx;
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_tsExpr);
						this.state = 854;
						if (!(this.precpred(this._ctx, 5))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 5)");
						}
						this.state = 855;
						this.match(BaseRqlParser.TS_MATH);
						this.state = 856;
						(_localctx as TsMathExpressionContext)._right = this.tsExpr(6);
						}
						break;

					case 2:
						{
						_localctx = new TsBinaryExpressionContext(new TsExprContext(_parentctx, _parentState));
						(_localctx as TsBinaryExpressionContext)._left = _prevctx;
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_tsExpr);
						this.state = 857;
						if (!(this.precpred(this._ctx, 4))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 4)");
						}
						this.state = 858;
						this.tsBinary();
						this.state = 859;
						(_localctx as TsBinaryExpressionContext)._right = this.tsExpr(5);
						}
						break;
					}
					}
				}
				this.state = 865;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 101, this._ctx);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsBetween(): TsBetweenContext {
		let _localctx: TsBetweenContext = new TsBetweenContext(this._ctx, this.state);
		this.enterRule(_localctx, 150, BaseRqlParser.RULE_tsBetween);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 866;
			this.match(BaseRqlParser.TS_BETWEEN);
			this.state = 867;
			_localctx._from = this.tsLiteral();
			this.state = 868;
			this.match(BaseRqlParser.TS_AND);
			this.state = 869;
			_localctx._to = this.tsLiteral();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsBinary(): TsBinaryContext {
		let _localctx: TsBinaryContext = new TsBinaryContext(this._ctx, this.state);
		this.enterRule(_localctx, 152, BaseRqlParser.RULE_tsBinary);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 871;
			_la = this._input.LA(1);
			if (!(_la === BaseRqlParser.TS_OR || _la === BaseRqlParser.TS_AND)) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsLiteral(): TsLiteralContext {
		let _localctx: TsLiteralContext = new TsLiteralContext(this._ctx, this.state);
		this.enterRule(_localctx, 154, BaseRqlParser.RULE_tsLiteral);
		let _la: number;
		try {
			let _alt: number;
			this.state = 896;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TS_DOL:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 873;
				this.match(BaseRqlParser.TS_DOL);
				this.state = 877;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case BaseRqlParser.TS_WORD:
					{
					this.state = 874;
					this.match(BaseRqlParser.TS_WORD);
					}
					break;
				case BaseRqlParser.TS_NUM:
					{
					this.state = 875;
					this.match(BaseRqlParser.TS_NUM);
					}
					break;
				case BaseRqlParser.TS_OR:
				case BaseRqlParser.TS_AND:
				case BaseRqlParser.TS_FROM:
				case BaseRqlParser.TS_WHERE:
				case BaseRqlParser.TS_GROUPBY:
				case BaseRqlParser.TS_TIMERANGE:
					{
					this.state = 876;
					this.tsIdentifiers();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case BaseRqlParser.TS_STRING:
			case BaseRqlParser.TS_WORD:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 879;
				_la = this._input.LA(1);
				if (!(_la === BaseRqlParser.TS_STRING || _la === BaseRqlParser.TS_WORD)) {
				this._errHandler.recoverInline(this);
				} else {
					if (this._input.LA(1) === Token.EOF) {
						this.matchedEOF = true;
					}

					this._errHandler.reportMatch(this);
					this.consume();
				}
				this.state = 883;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 103, this._ctx) ) {
				case 1:
					{
					this.state = 880;
					this.match(BaseRqlParser.TS_OP_Q);
					this.state = 881;
					this.match(BaseRqlParser.TS_NUM);
					this.state = 882;
					this.match(BaseRqlParser.TS_CL_Q);
					}
					break;
				}
				this.state = 893;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 105, this._ctx);
				while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
					if (_alt === 1) {
						{
						{
						this.state = 885;
						this.match(BaseRqlParser.TS_DOT);
						this.state = 889;
						this._errHandler.sync(this);
						switch (this._input.LA(1)) {
						case BaseRqlParser.TS_WORD:
							{
							this.state = 886;
							this.match(BaseRqlParser.TS_WORD);
							}
							break;
						case BaseRqlParser.TS_STRING:
							{
							this.state = 887;
							this.match(BaseRqlParser.TS_STRING);
							}
							break;
						case BaseRqlParser.TS_OR:
						case BaseRqlParser.TS_AND:
						case BaseRqlParser.TS_FROM:
						case BaseRqlParser.TS_WHERE:
						case BaseRqlParser.TS_GROUPBY:
						case BaseRqlParser.TS_TIMERANGE:
							{
							this.state = 888;
							this.tsIdentifiers();
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						}
						}
					}
					this.state = 895;
					this._errHandler.sync(this);
					_alt = this.interpreter.adaptivePredict(this._input, 105, this._ctx);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsTimeRangeFirst(): TsTimeRangeFirstContext {
		let _localctx: TsTimeRangeFirstContext = new TsTimeRangeFirstContext(this._ctx, this.state);
		this.enterRule(_localctx, 156, BaseRqlParser.RULE_tsTimeRangeFirst);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 898;
			this.match(BaseRqlParser.TS_FIRST);
			this.state = 899;
			_localctx._num = this.match(BaseRqlParser.TS_NUM);
			this.state = 900;
			_localctx._size = this.match(BaseRqlParser.TS_TIMERANGE);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsTimeRangeLast(): TsTimeRangeLastContext {
		let _localctx: TsTimeRangeLastContext = new TsTimeRangeLastContext(this._ctx, this.state);
		this.enterRule(_localctx, 158, BaseRqlParser.RULE_tsTimeRangeLast);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 902;
			this.match(BaseRqlParser.TS_LAST);
			this.state = 903;
			_localctx._num = this.match(BaseRqlParser.TS_NUM);
			this.state = 904;
			_localctx._size = this.match(BaseRqlParser.TS_TIMERANGE);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsCollectionName(): TsCollectionNameContext {
		let _localctx: TsCollectionNameContext = new TsCollectionNameContext(this._ctx, this.state);
		this.enterRule(_localctx, 160, BaseRqlParser.RULE_tsCollectionName);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 909;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TS_WORD:
				{
				this.state = 906;
				this.match(BaseRqlParser.TS_WORD);
				}
				break;
			case BaseRqlParser.TS_STRING:
				{
				this.state = 907;
				this.match(BaseRqlParser.TS_STRING);
				}
				break;
			case BaseRqlParser.TS_OR:
			case BaseRqlParser.TS_AND:
			case BaseRqlParser.TS_FROM:
			case BaseRqlParser.TS_WHERE:
			case BaseRqlParser.TS_GROUPBY:
			case BaseRqlParser.TS_TIMERANGE:
				{
				this.state = 908;
				this.tsIdentifiers();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 913;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_DOT) {
				{
				this.state = 911;
				this.match(BaseRqlParser.TS_DOT);
				this.state = 912;
				this.tsCollectionName();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsGroupBy(): TsGroupByContext {
		let _localctx: TsGroupByContext = new TsGroupByContext(this._ctx, this.state);
		this.enterRule(_localctx, 162, BaseRqlParser.RULE_tsGroupBy);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 915;
			this.match(BaseRqlParser.TS_GROUPBY);
			this.state = 916;
			_localctx._name = this.tsCollectionName();
			this.state = 921;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.TS_COMMA) {
				{
				{
				this.state = 917;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 918;
				this.tsCollectionName();
				}
				}
				this.state = 923;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 925;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_WITH) {
				{
				this.state = 924;
				this.match(BaseRqlParser.TS_WITH);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsSelect(): TsSelectContext {
		let _localctx: TsSelectContext = new TsSelectContext(this._ctx, this.state);
		this.enterRule(_localctx, 164, BaseRqlParser.RULE_tsSelect);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 927;
			this.match(BaseRqlParser.TS_SELECT);
			this.state = 928;
			_localctx._field = this.tsSelectVariable();
			this.state = 933;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.TS_COMMA) {
				{
				{
				this.state = 929;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 930;
				this.tsSelectVariable();
				}
				}
				this.state = 935;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsSelectScaleProjection(): TsSelectScaleProjectionContext {
		let _localctx: TsSelectScaleProjectionContext = new TsSelectScaleProjectionContext(this._ctx, this.state);
		this.enterRule(_localctx, 166, BaseRqlParser.RULE_tsSelectScaleProjection);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 936;
			this.match(BaseRqlParser.TS_SCALE);
			this.state = 937;
			this.match(BaseRqlParser.TS_NUM);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsSelectVariable(): TsSelectVariableContext {
		let _localctx: TsSelectVariableContext = new TsSelectVariableContext(this._ctx, this.state);
		this.enterRule(_localctx, 168, BaseRqlParser.RULE_tsSelectVariable);
		try {
			this.state = 941;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TS_METHOD:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 939;
				this.match(BaseRqlParser.TS_METHOD);
				}
				break;
			case BaseRqlParser.TS_DOL:
			case BaseRqlParser.TS_STRING:
			case BaseRqlParser.TS_WORD:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 940;
				this.tsLiteral();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tsIdentifiers(): TsIdentifiersContext {
		let _localctx: TsIdentifiersContext = new TsIdentifiersContext(this._ctx, this.state);
		this.enterRule(_localctx, 170, BaseRqlParser.RULE_tsIdentifiers);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 943;
			_la = this._input.LA(1);
			if (!(((((_la - 97)) & ~0x1F) === 0 && ((1 << (_la - 97)) & ((1 << (BaseRqlParser.TS_OR - 97)) | (1 << (BaseRqlParser.TS_AND - 97)) | (1 << (BaseRqlParser.TS_FROM - 97)) | (1 << (BaseRqlParser.TS_WHERE - 97)) | (1 << (BaseRqlParser.TS_GROUPBY - 97)) | (1 << (BaseRqlParser.TS_TIMERANGE - 97)))) !== 0))) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public updateBody(): UpdateBodyContext {
		let _localctx: UpdateBodyContext = new UpdateBodyContext(this._ctx, this.state);
		this.enterRule(_localctx, 172, BaseRqlParser.RULE_updateBody);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 945;
			this.match(BaseRqlParser.US_OP);
			this.state = 949;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.US_OP) {
				{
				{
				this.state = 946;
				this.updateBody();
				}
				}
				this.state = 951;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 952;
			this.match(BaseRqlParser.US_CL);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public filterStatement(): FilterStatementContext {
		let _localctx: FilterStatementContext = new FilterStatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 174, BaseRqlParser.RULE_filterStatement);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 954;
			this.filterMode();
			this.state = 955;
			this.filterExpr(0);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}

	public filterExpr(): FilterExprContext;
	public filterExpr(_p: number): FilterExprContext;
	// @RuleVersion(0)
	public filterExpr(_p?: number): FilterExprContext {
		if (_p === undefined) {
			_p = 0;
		}

		let _parentctx: ParserRuleContext = this._ctx;
		let _parentState: number = this.state;
		let _localctx: FilterExprContext = new FilterExprContext(this._ctx, _parentState);
		let _prevctx: FilterExprContext = _localctx;
		let _startState: number = 176;
		this.enterRecursionRule(_localctx, 176, BaseRqlParser.RULE_filterExpr, _p);
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 977;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 115, this._ctx) ) {
			case 1:
				{
				_localctx = new FilterOpParContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;

				this.state = 958;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 959;
				this.filterExpr(0);
				this.state = 960;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;

			case 2:
				{
				_localctx = new FilterEqualExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 962;
				(_localctx as FilterEqualExpressionContext)._left = this.exprValue();
				this.state = 963;
				this.match(BaseRqlParser.EQUAL);
				this.state = 964;
				(_localctx as FilterEqualExpressionContext)._right = this.exprValue();
				}
				break;

			case 3:
				{
				_localctx = new FilterMathExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 966;
				(_localctx as FilterMathExpressionContext)._left = this.exprValue();
				this.state = 967;
				this.match(BaseRqlParser.MATH);
				this.state = 968;
				(_localctx as FilterMathExpressionContext)._right = this.exprValue();
				}
				break;

			case 4:
				{
				_localctx = new FilterNormalFuncContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 970;
				(_localctx as FilterNormalFuncContext)._funcExpr = this.function();
				}
				break;

			case 5:
				{
				_localctx = new FilterBooleanExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 971;
				this.match(BaseRqlParser.TRUE);
				this.state = 972;
				this.match(BaseRqlParser.AND);
				this.state = 974;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 114, this._ctx) ) {
				case 1:
					{
					this.state = 973;
					this.match(BaseRqlParser.NOT);
					}
					break;
				}
				this.state = 976;
				this.expr(0);
				}
				break;
			}
			this._ctx._stop = this._input.tryLT(-1);
			this.state = 985;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 116, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = _localctx;
					{
					{
					_localctx = new FilterBinaryExpressionContext(new FilterExprContext(_parentctx, _parentState));
					(_localctx as FilterBinaryExpressionContext)._left = _prevctx;
					this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_filterExpr);
					this.state = 979;
					if (!(this.precpred(this._ctx, 6))) {
						throw this.createFailedPredicateException("this.precpred(this._ctx, 6)");
					}
					this.state = 980;
					this.binary();
					this.state = 981;
					(_localctx as FilterBinaryExpressionContext)._right = this.filterExpr(7);
					}
					}
				}
				this.state = 987;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 116, this._ctx);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public filterMode(): FilterModeContext {
		let _localctx: FilterModeContext = new FilterModeContext(this._ctx, this.state);
		this.enterRule(_localctx, 178, BaseRqlParser.RULE_filterMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 988;
			this.match(BaseRqlParser.FILTER);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public parameterBeforeQuery(): ParameterBeforeQueryContext {
		let _localctx: ParameterBeforeQueryContext = new ParameterBeforeQueryContext(this._ctx, this.state);
		this.enterRule(_localctx, 180, BaseRqlParser.RULE_parameterBeforeQuery);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 990;
			_localctx._name = this.literal();
			this.state = 991;
			this.match(BaseRqlParser.EQUAL);
			this.state = 992;
			_localctx._value = this.parameterValue();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public parameterValue(): ParameterValueContext {
		let _localctx: ParameterValueContext = new ParameterValueContext(this._ctx, this.state);
		this.enterRule(_localctx, 182, BaseRqlParser.RULE_parameterValue);
		try {
			this.state = 996;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 994;
				this.string();
				}
				break;
			case BaseRqlParser.NUM:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 995;
				this.match(BaseRqlParser.NUM);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public json(): JsonContext {
		let _localctx: JsonContext = new JsonContext(this._ctx, this.state);
		this.enterRule(_localctx, 184, BaseRqlParser.RULE_json);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 998;
			this.jsonObj();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public jsonObj(): JsonObjContext {
		let _localctx: JsonObjContext = new JsonObjContext(this._ctx, this.state);
		this.enterRule(_localctx, 186, BaseRqlParser.RULE_jsonObj);
		let _la: number;
		try {
			this.state = 1013;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 119, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 1000;
				this.match(BaseRqlParser.OP_CUR);
				this.state = 1001;
				this.jsonPair();
				this.state = 1006;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === BaseRqlParser.COMMA) {
					{
					{
					this.state = 1002;
					this.match(BaseRqlParser.COMMA);
					this.state = 1003;
					this.jsonPair();
					}
					}
					this.state = 1008;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 1009;
				this.match(BaseRqlParser.CL_CUR);
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 1011;
				this.match(BaseRqlParser.OP_CUR);
				this.state = 1012;
				this.match(BaseRqlParser.CL_CUR);
				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public jsonPair(): JsonPairContext {
		let _localctx: JsonPairContext = new JsonPairContext(this._ctx, this.state);
		this.enterRule(_localctx, 188, BaseRqlParser.RULE_jsonPair);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1015;
			this.match(BaseRqlParser.DOUBLE_QUOTE_STRING);
			this.state = 1016;
			this.match(BaseRqlParser.COLON);
			this.state = 1017;
			this.jsonValue();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public jsonArr(): JsonArrContext {
		let _localctx: JsonArrContext = new JsonArrContext(this._ctx, this.state);
		this.enterRule(_localctx, 190, BaseRqlParser.RULE_jsonArr);
		let _la: number;
		try {
			this.state = 1032;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 121, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 1019;
				this.match(BaseRqlParser.OP_Q);
				this.state = 1020;
				this.jsonValue();
				this.state = 1025;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === BaseRqlParser.COMMA) {
					{
					{
					this.state = 1021;
					this.match(BaseRqlParser.COMMA);
					this.state = 1022;
					this.jsonValue();
					}
					}
					this.state = 1027;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 1028;
				this.match(BaseRqlParser.CL_Q);
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 1030;
				this.match(BaseRqlParser.OP_Q);
				this.state = 1031;
				this.match(BaseRqlParser.CL_Q);
				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public jsonValue(): JsonValueContext {
		let _localctx: JsonValueContext = new JsonValueContext(this._ctx, this.state);
		this.enterRule(_localctx, 192, BaseRqlParser.RULE_jsonValue);
		try {
			this.state = 1041;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 1034;
				this.match(BaseRqlParser.DOUBLE_QUOTE_STRING);
				}
				break;
			case BaseRqlParser.NUM:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 1035;
				this.match(BaseRqlParser.NUM);
				}
				break;
			case BaseRqlParser.OP_CUR:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 1036;
				this.jsonObj();
				}
				break;
			case BaseRqlParser.OP_Q:
				this.enterOuterAlt(_localctx, 4);
				{
				this.state = 1037;
				this.jsonArr();
				}
				break;
			case BaseRqlParser.TRUE:
				this.enterOuterAlt(_localctx, 5);
				{
				this.state = 1038;
				this.match(BaseRqlParser.TRUE);
				}
				break;
			case BaseRqlParser.FALSE:
				this.enterOuterAlt(_localctx, 6);
				{
				this.state = 1039;
				this.match(BaseRqlParser.FALSE);
				}
				break;
			case BaseRqlParser.NULL:
				this.enterOuterAlt(_localctx, 7);
				{
				this.state = 1040;
				this.match(BaseRqlParser.NULL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public string(): StringContext {
		let _localctx: StringContext = new StringContext(this._ctx, this.state);
		this.enterRule(_localctx, 194, BaseRqlParser.RULE_string);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1043;
			_la = this._input.LA(1);
			if (!(_la === BaseRqlParser.DOUBLE_QUOTE_STRING || _la === BaseRqlParser.SINGLE_QUOTE_STRING)) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}

	public sempred(_localctx: RuleContext, ruleIndex: number, predIndex: number): boolean {
		switch (ruleIndex) {
		case 13:
			return this.expr_sempred(_localctx as ExprContext, predIndex);

		case 25:
			return this.specialParam_sempred(_localctx as SpecialParamContext, predIndex);

		case 74:
			return this.tsExpr_sempred(_localctx as TsExprContext, predIndex);

		case 88:
			return this.filterExpr_sempred(_localctx as FilterExprContext, predIndex);
		}
		return true;
	}
	private expr_sempred(_localctx: ExprContext, predIndex: number): boolean {
		switch (predIndex) {
		case 0:
			return this.precpred(this._ctx, 10);
		}
		return true;
	}
	private specialParam_sempred(_localctx: SpecialParamContext, predIndex: number): boolean {
		switch (predIndex) {
		case 1:
			return this.precpred(this._ctx, 13);

		case 2:
			return this.precpred(this._ctx, 11);

		case 3:
			return this.precpred(this._ctx, 10);

		case 4:
			return this.precpred(this._ctx, 9);
		}
		return true;
	}
	private tsExpr_sempred(_localctx: TsExprContext, predIndex: number): boolean {
		switch (predIndex) {
		case 5:
			return this.precpred(this._ctx, 5);

		case 6:
			return this.precpred(this._ctx, 4);
		}
		return true;
	}
	private filterExpr_sempred(_localctx: FilterExprContext, predIndex: number): boolean {
		switch (predIndex) {
		case 7:
			return this.precpred(this._ctx, 6);
		}
		return true;
	}

	private static readonly _serializedATNSegments: number = 2;
	private static readonly _serializedATNSegment0: string =
		"\x03\uC91D\uCABA\u058D\uAFBA\u4F53\u0607\uEA8B\uC241\x03\x86\u0418\x04" +
		"\x02\t\x02\x04\x03\t\x03\x04\x04\t\x04\x04\x05\t\x05\x04\x06\t\x06\x04" +
		"\x07\t\x07\x04\b\t\b\x04\t\t\t\x04\n\t\n\x04\v\t\v\x04\f\t\f\x04\r\t\r" +
		"\x04\x0E\t\x0E\x04\x0F\t\x0F\x04\x10\t\x10\x04\x11\t\x11\x04\x12\t\x12" +
		"\x04\x13\t\x13\x04\x14\t\x14\x04\x15\t\x15\x04\x16\t\x16\x04\x17\t\x17" +
		"\x04\x18\t\x18\x04\x19\t\x19\x04\x1A\t\x1A\x04\x1B\t\x1B\x04\x1C\t\x1C" +
		"\x04\x1D\t\x1D\x04\x1E\t\x1E\x04\x1F\t\x1F\x04 \t \x04!\t!\x04\"\t\"\x04" +
		"#\t#\x04$\t$\x04%\t%\x04&\t&\x04\'\t\'\x04(\t(\x04)\t)\x04*\t*\x04+\t" +
		"+\x04,\t,\x04-\t-\x04.\t.\x04/\t/\x040\t0\x041\t1\x042\t2\x043\t3\x04" +
		"4\t4\x045\t5\x046\t6\x047\t7\x048\t8\x049\t9\x04:\t:\x04;\t;\x04<\t<\x04" +
		"=\t=\x04>\t>\x04?\t?\x04@\t@\x04A\tA\x04B\tB\x04C\tC\x04D\tD\x04E\tE\x04" +
		"F\tF\x04G\tG\x04H\tH\x04I\tI\x04J\tJ\x04K\tK\x04L\tL\x04M\tM\x04N\tN\x04" +
		"O\tO\x04P\tP\x04Q\tQ\x04R\tR\x04S\tS\x04T\tT\x04U\tU\x04V\tV\x04W\tW\x04" +
		"X\tX\x04Y\tY\x04Z\tZ\x04[\t[\x04\\\t\\\x04]\t]\x04^\t^\x04_\t_\x04`\t" +
		"`\x04a\ta\x04b\tb\x04c\tc\x03\x02\x07\x02\xC8\n\x02\f\x02\x0E\x02\xCB" +
		"\v\x02\x03\x02\x07\x02\xCE\n\x02\f\x02\x0E\x02\xD1\v\x02\x03\x02\x03\x02" +
		"\x05\x02\xD5\n\x02\x03\x02\x05\x02\xD8\n\x02\x03\x02\x05\x02\xDB\n\x02" +
		"\x03\x02\x05\x02\xDE\n\x02\x03\x02\x05\x02\xE1\n\x02\x03\x02\x05\x02\xE4" +
		"\n\x02\x03\x02\x05\x02\xE7\n\x02\x03\x02\x05\x02\xEA\n\x02\x03\x02\x05" +
		"\x02\xED\n\x02\x03\x02\x05\x02\xF0\n\x02\x03\x02\x03\x02\x03\x03\x03\x03" +
		"\x05\x03\xF6\n\x03\x03\x04\x03\x04\x03\x04\x07\x04\xFB\n\x04\f\x04\x0E" +
		"\x04\xFE\v\x04\x03\x04\x03\x04\x03\x04\x03\x05\x03\x05\x03\x06\x03\x06" +
		"\x03\x06\x03\x06\x05\x06\u0109\n\x06\x03\x06\x03\x06\x03\x06\x03\x06\x03" +
		"\x06\x05\x06\u0110\n\x06\x05\x06\u0112\n\x06\x03\x07\x03\x07\x03\x07\x05" +
		"\x07\u0117\n\x07\x03\b\x03\b\x03\b\x05\b\u011C\n\b\x03\t\x05\t\u011F\n" +
		"\t\x03\t\x03\t\x05\t\u0123\n\t\x03\n\x03\n\x03\v\x03\v\x03\v\x03\v\x07" +
		"\v\u012B\n\v\f\v\x0E\v\u012E\v\v\x03\f\x03\f\x03\r\x03\r\x03\x0E\x03\x0E" +
		"\x03\x0E\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F" +
		"\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F" +
		"\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x05\x0F\u014C\n\x0F\x03\x0F\x05\x0F\u014F" +
		"\n\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x07\x0F\u0155\n\x0F\f\x0F\x0E\x0F" +
		"\u0158\v\x0F\x03\x10\x03\x10\x03\x10\x03\x10\x03\x10\x03\x10\x05\x10\u0160" +
		"\n\x10\x03\x11\x03\x11\x03\x12\x03\x12\x05\x12\u0166\n\x12\x03\x12\x03" +
		"\x12\x03\x12\x03\x12\x03\x12\x07\x12\u016D\n\x12\f\x12\x0E\x12\u0170\v" +
		"\x12\x03\x12\x03\x12\x03\x13\x03\x13\x03\x13\x03\x13\x03\x13\x03\x13\x03" +
		"\x14\x03\x14\x03\x14\x03\x14\x05\x14\u017E\n\x14\x03\x14\x03\x14\x03\x14" +
		"\x05\x14\u0183\n\x14\x07\x14\u0185\n\x14\f\x14\x0E\x14\u0188\v\x14\x05" +
		"\x14\u018A\n\x14\x03\x14\x03\x14\x03\x15\x03\x15\x03\x16\x03\x16\x03\x16" +
		"\x03\x17\x03\x17\x03\x17\x03\x17\x03\x17\x03\x17\x03\x17\x05\x17\u019A" +
		"\n\x17\x03\x17\x03\x17\x03\x17\x05\x17\u019F\n\x17\x03\x17\x03\x17\x03" +
		"\x17\x03\x17\x03\x17\x03\x17\x03\x17\x05\x17\u01A8\n\x17\x03\x17\x03\x17" +
		"\x03\x17\x03\x17\x05\x17\u01AE\n\x17\x03\x17\x03\x17\x05\x17\u01B2\n\x17" +
		"\x03\x18\x03\x18\x03\x18\x03\x18\x03\x18\x03\x18\x03\x19\x03\x19\x03\x19" +
		"\x03\x19\x05\x19\u01BE\n\x19\x03\x1A\x03\x1A\x03\x1A\x03\x1A\x03\x1A\x03" +
		"\x1A\x05\x1A\u01C6\n\x1A\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B" +
		"\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B" +
		"\x03\x1B\x03\x1B\x05\x1B\u01D9\n\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03" +
		"\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x07\x1B\u01E7" +
		"\n\x1B\f\x1B\x0E\x1B\u01EA\v\x1B\x03\x1C\x03\x1C\x03\x1D\x03\x1D\x03\x1D" +
		"\x03\x1D\x07\x1D\u01F2\n\x1D\f\x1D\x0E\x1D\u01F5\v\x1D\x03\x1E\x03\x1E" +
		"\x03\x1E\x03\x1F\x03\x1F\x03 \x03 \x03 \x03 \x07 \u0200\n \f \x0E \u0203" +
		"\v \x03!\x03!\x05!\u0207\n!\x03!\x05!\u020A\n!\x03!\x05!\u020D\n!\x03" +
		"\"\x03\"\x03\"\x03#\x03#\x03$\x03$\x03%\x03%\x03&\x03&\x03\'\x03\'\x03" +
		"\'\x03\'\x05\'\u021E\n\'\x03\'\x03\'\x05\'\u0222\n\'\x03\'\x03\'\x03\'" +
		"\x07\'\u0227\n\'\f\'\x0E\'\u022A\v\'\x03\'\x05\'\u022D\n\'\x03\'\x03\'" +
		"\x07\'\u0231\n\'\f\'\x0E\'\u0234\v\'\x03\'\x03\'\x05\'\u0238\n\'\x05\'" +
		"\u023A\n\'\x03(\x03(\x03(\x05(\u023F\n(\x03(\x05(\u0242\n(\x03)\x03)\x03" +
		")\x03)\x05)\u0248\n)\x03)\x03)\x07)\u024C\n)\f)\x0E)\u024F\v)\x03)\x03" +
		")\x03)\x07)\u0254\n)\f)\x0E)\u0257\v)\x03)\x03)\x03*\x03*\x07*\u025D\n" +
		"*\f*\x0E*\u0260\v*\x03*\x03*\x03+\x03+\x03+\x05+\u0267\n+\x03,\x03,\x03" +
		",\x05,\u026C\n,\x03-\x03-\x03-\x03-\x05-\u0272\n-\x03-\x05-\u0275\n-\x03" +
		"-\x06-\u0278\n-\r-\x0E-\u0279\x05-\u027C\n-\x03.\x03.\x03.\x03/\x03/\x03" +
		"0\x030\x030\x050\u0286\n0\x030\x030\x030\x050\u028B\n0\x070\u028D\n0\f" +
		"0\x0E0\u0290\v0\x031\x031\x031\x031\x051\u0296\n1\x031\x031\x051\u029A" +
		"\n1\x031\x031\x051\u029E\n1\x032\x032\x032\x032\x032\x052\u02A5\n2\x03" +
		"3\x033\x053\u02A9\n3\x034\x034\x034\x034\x034\x034\x034\x034\x054\u02B3" +
		"\n4\x034\x054\u02B6\n4\x035\x055\u02B9\n5\x035\x035\x055\u02BD\n5\x03" +
		"6\x036\x036\x037\x037\x057\u02C4\n7\x038\x038\x058\u02C8\n8\x039\x039" +
		"\x039\x059\u02CD\n9\x039\x039\x03:\x03:\x03:\x07:\u02D4\n:\f:\x0E:\u02D7" +
		"\v:\x03;\x03;\x03<\x03<\x03=\x03=\x05=\u02DF\n=\x03>\x03>\x03>\x05>\u02E4" +
		"\n>\x03>\x03>\x03>\x05>\u02E9\n>\x03>\x03>\x03?\x03?\x03?\x03?\x03@\x03" +
		"@\x03@\x03@\x03A\x03A\x03A\x03A\x05A\u02F9\nA\x03A\x03A\x05A\u02FD\nA" +
		"\x03A\x03A\x03B\x03B\x05B\u0303\nB\x03C\x03C\x03C\x03C\x03C\x05C\u030A" +
		"\nC\x03C\x03C\x03D\x03D\x05D\u0310\nD\x03D\x05D\u0313\nD\x03D\x05D\u0316" +
		"\nD\x03D\x05D\u0319\nD\x03D\x05D\u031C\nD\x03D\x05D\u031F\nD\x03D\x05" +
		"D\u0322\nD\x03E\x03E\x03E\x03F\x03F\x03F\x03F\x03F\x03F\x07F\u032D\nF" +
		"\fF\x0EF\u0330\vF\x03F\x03F\x03F\x03F\x03F\x03G\x03G\x03G\x03G\x03G\x03" +
		"G\x05G\u033D\nG\x03H\x03H\x03H\x03I\x03I\x03I\x03J\x03J\x03J\x03K\x03" +
		"K\x03K\x03L\x03L\x03L\x03L\x03L\x03L\x03L\x03L\x05L\u0353\nL\x03L\x03" +
		"L\x05L\u0357\nL\x03L\x03L\x03L\x03L\x03L\x03L\x03L\x07L\u0360\nL\fL\x0E" +
		"L\u0363\vL\x03M\x03M\x03M\x03M\x03M\x03N\x03N\x03O\x03O\x03O\x03O\x05" +
		"O\u0370\nO\x03O\x03O\x03O\x03O\x05O\u0376\nO\x03O\x03O\x03O\x03O\x05O" +
		"\u037C\nO\x07O\u037E\nO\fO\x0EO\u0381\vO\x05O\u0383\nO\x03P\x03P\x03P" +
		"\x03P\x03Q\x03Q\x03Q\x03Q\x03R\x03R\x03R\x05R\u0390\nR\x03R\x03R\x05R" +
		"\u0394\nR\x03S\x03S\x03S\x03S\x07S\u039A\nS\fS\x0ES\u039D\vS\x03S\x05" +
		"S\u03A0\nS\x03T\x03T\x03T\x03T\x07T\u03A6\nT\fT\x0ET\u03A9\vT\x03U\x03" +
		"U\x03U\x03V\x03V\x05V\u03B0\nV\x03W\x03W\x03X\x03X\x07X\u03B6\nX\fX\x0E" +
		"X\u03B9\vX\x03X\x03X\x03Y\x03Y\x03Y\x03Z\x03Z\x03Z\x03Z\x03Z\x03Z\x03" +
		"Z\x03Z\x03Z\x03Z\x03Z\x03Z\x03Z\x03Z\x03Z\x03Z\x03Z\x05Z\u03D1\nZ\x03" +
		"Z\x05Z\u03D4\nZ\x03Z\x03Z\x03Z\x03Z\x07Z\u03DA\nZ\fZ\x0EZ\u03DD\vZ\x03" +
		"[\x03[\x03\\\x03\\\x03\\\x03\\\x03]\x03]\x05]\u03E7\n]\x03^\x03^\x03_" +
		"\x03_\x03_\x03_\x07_\u03EF\n_\f_\x0E_\u03F2\v_\x03_\x03_\x03_\x03_\x05" +
		"_\u03F8\n_\x03`\x03`\x03`\x03`\x03a\x03a\x03a\x03a\x07a\u0402\na\fa\x0E" +
		"a\u0405\va\x03a\x03a\x03a\x03a\x05a\u040B\na\x03b\x03b\x03b\x03b\x03b" +
		"\x03b\x03b\x05b\u0414\nb\x03c\x03c\x03c\x02\x02\x06\x1C4\x96\xB2d\x02" +
		"\x02\x04\x02\x06\x02\b\x02\n\x02\f\x02\x0E\x02\x10\x02\x12\x02\x14\x02" +
		"\x16\x02\x18\x02\x1A\x02\x1C\x02\x1E\x02 \x02\"\x02$\x02&\x02(\x02*\x02" +
		",\x02.\x020\x022\x024\x026\x028\x02:\x02<\x02>\x02@\x02B\x02D\x02F\x02" +
		"H\x02J\x02L\x02N\x02P\x02R\x02T\x02V\x02X\x02Z\x02\\\x02^\x02`\x02b\x02" +
		"d\x02f\x02h\x02j\x02l\x02n\x02p\x02r\x02t\x02v\x02x\x02z\x02|\x02~\x02" +
		"\x80\x02\x82\x02\x84\x02\x86\x02\x88\x02\x8A\x02\x8C\x02\x8E\x02\x90\x02" +
		"\x92\x02\x94\x02\x96\x02\x98\x02\x9A\x02\x9C\x02\x9E\x02\xA0\x02\xA2\x02" +
		"\xA4\x02\xA6\x02\xA8\x02\xAA\x02\xAC\x02\xAE\x02\xB0\x02\xB2\x02\xB4\x02" +
		"\xB6\x02\xB8\x02\xBA\x02\xBC\x02\xBE\x02\xC0\x02\xC2\x02\xC4\x02\x02\x0E" +
		"\t\x02$%\'\'**//44DFMM\x04\x0266SS\x06\x02\x1E\x1E##11??\x03\x029:\x04" +
		"\x02\x06\x06;;\v\x02\x1C\x1C\x1E\x1F!\'*+//17>ACFMM\v\x02(),,..0088<<" +
		"BBLLNO\x03\x02lm\x04\x02ccgg\x04\x02vvxx\x05\x02ccgjoo\x03\x02TU\x02\u045B" +
		"\x02\xC9\x03\x02\x02\x02\x04\xF5\x03\x02\x02\x02\x06\xF7\x03\x02\x02\x02" +
		"\b\u0102\x03\x02\x02\x02\n\u0111\x03\x02\x02\x02\f\u0116\x03\x02\x02\x02" +
		"\x0E\u011B\x03\x02\x02\x02\x10\u011E\x03\x02\x02\x02\x12\u0124\x03\x02" +
		"\x02\x02\x14\u0126\x03\x02\x02\x02\x16\u012F\x03\x02\x02\x02\x18\u0131" +
		"\x03\x02\x02\x02\x1A\u0133\x03\x02\x02\x02\x1C\u014E\x03\x02\x02\x02\x1E" +
		"\u015F\x03\x02\x02\x02 \u0161\x03\x02\x02\x02\"\u0163\x03\x02\x02\x02" +
		"$\u0173\x03\x02\x02\x02&\u0179\x03\x02\x02\x02(\u018D\x03\x02\x02\x02" +
		"*\u018F\x03\x02\x02\x02,\u01B1\x03\x02\x02\x02.\u01B3\x03\x02\x02\x02" +
		"0\u01B9\x03\x02\x02\x022\u01C5\x03\x02\x02\x024\u01D8\x03\x02\x02\x02" +
		"6\u01EB\x03\x02\x02\x028\u01ED\x03\x02\x02\x02:\u01F6\x03\x02\x02\x02" +
		"<\u01F9\x03\x02\x02\x02>\u01FB\x03\x02\x02\x02@\u0204\x03\x02\x02\x02" +
		"B\u020E\x03\x02\x02\x02D\u0211\x03\x02\x02\x02F\u0213\x03\x02\x02\x02" +
		"H\u0215\x03\x02\x02\x02J\u0217\x03\x02\x02\x02L\u0239\x03\x02\x02\x02" +
		"N\u023E\x03\x02\x02\x02P\u0243\x03\x02\x02\x02R\u025A\x03\x02\x02\x02" +
		"T\u0263\x03\x02\x02\x02V\u026B\x03\x02\x02\x02X\u027B\x03\x02\x02\x02" +
		"Z\u027D\x03\x02\x02\x02\\\u0280\x03\x02\x02\x02^\u0282\x03\x02\x02\x02" +
		"`\u029D\x03\x02\x02\x02b\u02A4\x03\x02\x02\x02d\u02A8\x03\x02\x02\x02" +
		"f\u02B2\x03\x02\x02\x02h\u02B8\x03\x02\x02\x02j\u02BE\x03\x02\x02\x02" +
		"l\u02C1\x03\x02\x02\x02n\u02C7\x03\x02\x02\x02p\u02C9\x03\x02\x02\x02" +
		"r\u02D0\x03\x02\x02\x02t\u02D8\x03\x02\x02\x02v\u02DA\x03\x02\x02\x02" +
		"x\u02DE\x03\x02\x02\x02z\u02E0\x03\x02\x02\x02|\u02EC\x03\x02\x02\x02" +
		"~\u02F0\x03\x02\x02\x02\x80\u02F4\x03\x02\x02\x02\x82\u0302\x03\x02\x02" +
		"\x02\x84\u0304\x03\x02\x02\x02\x86\u030D\x03\x02\x02\x02\x88\u0323\x03" +
		"\x02\x02\x02\x8A\u0326\x03\x02\x02\x02\x8C\u033C\x03\x02\x02\x02\x8E\u033E" +
		"\x03\x02\x02\x02\x90\u0341\x03\x02\x02\x02\x92\u0344\x03\x02\x02\x02\x94" +
		"\u0347\x03\x02\x02\x02\x96\u0356\x03\x02\x02\x02\x98\u0364\x03\x02\x02" +
		"\x02\x9A\u0369\x03\x02\x02\x02\x9C\u0382\x03\x02\x02\x02\x9E\u0384\x03" +
		"\x02\x02\x02\xA0\u0388\x03\x02\x02\x02\xA2\u038F\x03\x02\x02\x02\xA4\u0395" +
		"\x03\x02\x02\x02\xA6\u03A1\x03\x02\x02\x02\xA8\u03AA\x03\x02\x02\x02\xAA" +
		"\u03AF\x03\x02\x02\x02\xAC\u03B1\x03\x02\x02\x02\xAE\u03B3\x03\x02\x02" +
		"\x02\xB0\u03BC\x03\x02\x02\x02\xB2\u03D3\x03\x02\x02\x02\xB4\u03DE\x03" +
		"\x02\x02\x02\xB6\u03E0\x03\x02\x02\x02\xB8\u03E6\x03\x02\x02\x02\xBA\u03E8" +
		"\x03\x02\x02\x02\xBC\u03F7\x03\x02\x02\x02\xBE\u03F9\x03\x02\x02\x02\xC0" +
		"\u040A\x03\x02\x02\x02\xC2\u0413\x03\x02\x02\x02\xC4\u0415\x03\x02\x02" +
		"\x02\xC6\xC8\x05\xB6\\\x02\xC7\xC6\x03\x02\x02\x02\xC8\xCB\x03\x02\x02" +
		"\x02\xC9\xC7\x03\x02\x02\x02\xC9\xCA\x03\x02\x02\x02\xCA\xCF\x03\x02\x02" +
		"\x02\xCB\xC9\x03\x02\x02\x02\xCC\xCE\x05\x04\x03\x02\xCD\xCC\x03\x02\x02" +
		"\x02\xCE\xD1\x03\x02\x02\x02\xCF\xCD\x03\x02\x02\x02\xCF\xD0\x03\x02\x02" +
		"\x02\xD0\xD2\x03\x02\x02\x02\xD1\xCF\x03\x02\x02\x02\xD2\xD4\x05\n\x06" +
		"\x02\xD3\xD5\x05\x14\v\x02\xD4\xD3\x03\x02\x02\x02\xD4\xD5\x03\x02\x02" +
		"\x02\xD5\xD7\x03\x02\x02\x02\xD6\xD8\x05\x1A\x0E\x02\xD7\xD6\x03\x02\x02" +
		"\x02\xD7\xD8\x03\x02\x02\x02\xD8\xDA\x03\x02\x02\x02\xD9\xDB\x05> \x02" +
		"\xDA\xD9\x03\x02\x02\x02\xDA\xDB\x03\x02\x02\x02\xDB\xDD\x03\x02\x02\x02" +
		"\xDC\xDE\x058\x1D\x02\xDD\xDC\x03\x02\x02\x02\xDD\xDE\x03\x02\x02\x02" +
		"\xDE\xE0\x03\x02\x02\x02\xDF\xE1\x05\x06\x04\x02\xE0\xDF\x03\x02\x02\x02" +
		"\xE0\xE1\x03\x02\x02\x02\xE1\xE3\x03\x02\x02\x02\xE2\xE4\x05\xB0Y\x02" +
		"\xE3\xE2\x03\x02\x02\x02\xE3\xE4\x03\x02\x02\x02\xE4\xE6\x03\x02\x02\x02" +
		"\xE5\xE7\x05L\'\x02\xE6\xE5\x03\x02\x02\x02\xE6\xE7\x03\x02\x02\x02\xE7" +
		"\xE9\x03\x02\x02\x02\xE8\xEA\x05^0\x02\xE9\xE8\x03\x02\x02\x02\xE9\xEA" +
		"\x03\x02\x02\x02\xEA\xEC\x03\x02\x02\x02\xEB\xED\x05`1\x02\xEC\xEB\x03" +
		"\x02\x02\x02\xEC\xED\x03\x02\x02\x02\xED\xEF\x03\x02\x02\x02\xEE\xF0\x05" +
		"\xBA^\x02\xEF\xEE\x03\x02\x02\x02\xEF\xF0\x03\x02\x02\x02\xF0\xF1\x03" +
		"\x02\x02\x02\xF1\xF2\x07\x02\x02\x03\xF2\x03\x03\x02\x02\x02\xF3\xF6\x05" +
		"P)\x02\xF4\xF6\x05\x8AF\x02\xF5\xF3\x03\x02\x02\x02\xF5\xF4\x03\x02\x02" +
		"\x02\xF6\x05\x03\x02\x02\x02\xF7\xF8\x07-\x02\x02\xF8\xFC\x07z\x02\x02" +
		"\xF9\xFB\x05\xAEX\x02\xFA\xF9\x03\x02\x02\x02\xFB\xFE\x03\x02\x02\x02" +
		"\xFC\xFA\x03\x02\x02\x02\xFC\xFD\x03\x02\x02\x02\xFD\xFF\x03\x02\x02\x02" +
		"\xFE\xFC\x03\x02\x02\x02\xFF\u0100\x07{\x02\x02\u0100\u0101\x07\x02\x02" +
		"\x03\u0101\x07\x03\x02\x02\x02\u0102\u0103\x07(\x02\x02\u0103\t\x03\x02" +
		"\x02\x02\u0104\u0105\x05\b\x05\x02\u0105\u0106\x07.\x02\x02\u0106\u0108" +
		"\x05\f\x07\x02\u0107\u0109\x05\x10\t\x02\u0108\u0107\x03\x02\x02\x02\u0108" +
		"\u0109\x03\x02\x02\x02\u0109\u0112\x03\x02\x02\x02\u010A\u010B\x07(\x02" +
		"\x02\u010B\u0112\x07\x1D\x02\x02\u010C\u010D\x05\b\x05\x02\u010D\u010F" +
		"\x05\x0E\b\x02\u010E\u0110\x05\x10\t\x02\u010F\u010E\x03\x02\x02\x02\u010F" +
		"\u0110\x03\x02\x02\x02\u0110\u0112\x03\x02\x02\x02\u0111\u0104\x03\x02" +
		"\x02\x02\u0111\u010A\x03\x02\x02\x02\u0111\u010C\x03\x02\x02\x02\u0112" +
		"\v\x03\x02\x02\x02\u0113\u0117\x07V\x02\x02\u0114\u0117\x05\xC4c\x02\u0115" +
		"\u0117\x05t;\x02\u0116\u0113\x03\x02\x02\x02\u0116\u0114\x03\x02\x02\x02" +
		"\u0116\u0115\x03\x02\x02\x02\u0117\r\x03\x02\x02\x02\u0118\u011C\x07V" +
		"\x02\x02\u0119\u011C\x05\xC4c\x02\u011A\u011C\x05t;\x02\u011B\u0118\x03" +
		"\x02\x02\x02\u011B\u0119\x03\x02\x02\x02\u011B\u011A\x03\x02\x02\x02\u011C" +
		"\x0F\x03\x02\x02\x02\u011D\u011F\x07 \x02\x02\u011E\u011D\x03\x02\x02" +
		"\x02\u011E\u011F\x03\x02\x02\x02\u011F\u0120\x03\x02\x02\x02\u0120\u0122" +
		"\x05V,\x02\u0121\u0123\x05Z.\x02\u0122\u0121\x03\x02\x02\x02\u0122\u0123" +
		"\x03\x02\x02\x02\u0123\x11\x03\x02\x02\x02\u0124\u0125\x07)\x02\x02\u0125" +
		"\x13\x03\x02\x02\x02\u0126\u0127\x05\x12\n\x02\u0127\u012C\x05l7\x02\u0128" +
		"\u0129\x07\x06\x02\x02\u0129\u012B\x05l7\x02\u012A\u0128\x03\x02\x02\x02" +
		"\u012B\u012E\x03\x02\x02\x02\u012C\u012A\x03\x02\x02\x02\u012C\u012D\x03" +
		"\x02\x02\x02\u012D\x15\x03\x02\x02\x02\u012E\u012C\x03\x02\x02\x02\u012F" +
		"\u0130\x03\x02\x02\x02\u0130\x17\x03\x02\x02\x02\u0131\u0132\x07B\x02" +
		"\x02\u0132\x19\x03\x02\x02\x02\u0133\u0134\x05\x18\r\x02\u0134\u0135\x05" +
		"\x1C\x0F\x02\u0135\x1B\x03\x02\x02\x02\u0136\u0137\b\x0F\x01\x02\u0137" +
		"\u0138\x07\v\x02\x02\u0138\u0139\x05\x1C\x0F\x02\u0139\u013A\x07\x04\x02" +
		"\x02\u013A\u014F\x03\x02\x02\x02\u013B\u013C\x05 \x11\x02\u013C\u013D" +
		"\x07\b\x02\x02\u013D\u013E\x05 \x11\x02\u013E\u014F\x03\x02\x02\x02\u013F" +
		"\u0140\x05 \x11\x02\u0140\u0141\x07\t\x02\x02\u0141\u0142\x05 \x11\x02" +
		"\u0142\u014F\x03\x02\x02\x02\u0143\u014F\x05&\x14\x02\u0144\u014F\x05" +
		"\"\x12\x02\u0145\u014F\x05*\x16\x02\u0146\u014F\x05$\x13\x02\u0147\u014F" +
		"\x05p9\x02\u0148\u0149\x07A\x02\x02\u0149\u014B\x07\x1F\x02\x02\u014A" +
		"\u014C\x075\x02\x02\u014B\u014A\x03\x02\x02\x02\u014B\u014C\x03\x02\x02" +
		"\x02\u014C\u014D\x03\x02\x02\x02\u014D\u014F\x05\x1C\x0F\x03\u014E\u0136" +
		"\x03\x02\x02\x02\u014E\u013B\x03\x02\x02\x02\u014E\u013F\x03\x02\x02\x02" +
		"\u014E\u0143\x03\x02\x02\x02\u014E\u0144\x03\x02\x02\x02\u014E\u0145\x03" +
		"\x02\x02\x02\u014E\u0146\x03\x02\x02\x02\u014E\u0147\x03\x02\x02\x02\u014E" +
		"\u0148\x03\x02\x02\x02\u014F\u0156\x03\x02\x02\x02\u0150\u0151\f\f\x02" +
		"\x02\u0151\u0152\x05\x1E\x10\x02\u0152\u0153\x05\x1C\x0F\r\u0153\u0155" +
		"\x03\x02\x02\x02\u0154\u0150\x03\x02\x02\x02\u0155\u0158\x03\x02\x02\x02" +
		"\u0156\u0154\x03\x02\x02\x02\u0156\u0157\x03\x02\x02\x02\u0157\x1D\x03" +
		"\x02\x02\x02\u0158\u0156\x03\x02\x02\x02\u0159\u015A\x07\x1F\x02\x02\u015A" +
		"\u0160\x075\x02\x02\u015B\u015C\x077\x02\x02\u015C\u0160\x075\x02\x02" +
		"\u015D\u0160\x07\x1F\x02\x02\u015E\u0160\x077\x02\x02\u015F\u0159\x03" +
		"\x02\x02\x02\u015F\u015B\x03\x02\x02\x02\u015F\u015D\x03\x02\x02\x02\u015F" +
		"\u015E\x03\x02\x02\x02\u0160\x1F\x03\x02\x02\x02\u0161\u0162\x05h5\x02" +
		"\u0162!\x03\x02\x02\x02\u0163\u0165\x05h5\x02\u0164\u0166\x07\x1C\x02" +
		"\x02\u0165\u0164\x03\x02\x02\x02\u0165\u0166\x03\x02\x02\x02\u0166\u0167" +
		"\x03\x02\x02\x02\u0167\u0168\x07+\x02\x02\u0168\u0169\x07\v\x02\x02\u0169" +
		"\u016E\x05h5\x02\u016A\u016B\x07\x06\x02\x02\u016B\u016D\x05h5\x02\u016C" +
		"\u016A\x03\x02\x02\x02\u016D\u0170\x03\x02\x02\x02\u016E\u016C\x03\x02" +
		"\x02\x02\u016E\u016F\x03\x02\x02\x02\u016F\u0171\x03\x02\x02\x02\u0170" +
		"\u016E\x03\x02\x02\x02\u0171\u0172\x07\x04\x02\x02\u0172#\x03\x02\x02" +
		"\x02\u0173\u0174\x05h5\x02\u0174\u0175\x07!\x02\x02\u0175\u0176\x05h5" +
		"\x02\u0176\u0177\x07\x1F\x02\x02\u0177\u0178\x05h5\x02\u0178%\x03\x02" +
		"\x02\x02\u0179\u017A\x05(\x15\x02\u017A\u0189\x07\v\x02\x02\u017B\u017D" +
		"\x054\x1B\x02\u017C\u017E\x05T+\x02\u017D\u017C\x03\x02\x02\x02\u017D" +
		"\u017E\x03\x02\x02\x02\u017E\u0186\x03\x02\x02\x02\u017F\u0180\x07\x06" +
		"\x02\x02\u0180\u0182\x054\x1B\x02\u0181\u0183\x05T+\x02\u0182\u0181\x03" +
		"\x02\x02\x02\u0182\u0183\x03\x02\x02\x02\u0183\u0185\x03\x02\x02\x02\u0184" +
		"\u017F\x03\x02\x02\x02\u0185\u0188\x03\x02\x02\x02\u0186\u0184\x03\x02" +
		"\x02\x02\u0186\u0187\x03\x02\x02\x02\u0187\u018A\x03\x02\x02\x02\u0188" +
		"\u0186\x03\x02\x02\x02\u0189\u017B\x03\x02\x02\x02\u0189\u018A\x03\x02" +
		"\x02\x02\u018A\u018B\x03\x02\x02\x02\u018B\u018C\x07\x04\x02\x02\u018C" +
		"\'\x03\x02\x02\x02\u018D\u018E\t\x02\x02\x02\u018E)\x03\x02\x02\x02\u018F" +
		"\u0190\x07G\x02\x02\u0190\u0191\x05,\x17\x02\u0191+\x03\x02\x02\x02\u0192" +
		"\u0199\x07\v\x02\x02\u0193\u0194\x07H\x02\x02\u0194\u0195\x07\v\x02\x02" +
		"\u0195\u0196\x05b2\x02\u0196\u0197\x07\x04\x02\x02\u0197\u019A\x03\x02" +
		"\x02\x02\u0198\u019A\x05h5\x02\u0199\u0193\x03\x02\x02\x02\u0199\u0198" +
		"\x03\x02\x02\x02\u019A\u019B\x03\x02\x02\x02\u019B\u019C\x07\x06\x02\x02" +
		"\u019C\u019E\x052\x1A\x02\u019D\u019F\x050\x19\x02\u019E\u019D\x03\x02" +
		"\x02\x02\u019E\u019F\x03\x02\x02\x02\u019F\u01A0\x03\x02\x02\x02\u01A0" +
		"\u01A1\x07\x04\x02\x02\u01A1\u01B2\x03\x02\x02\x02\u01A2\u01A3\x07\v\x02" +
		"\x02\u01A3\u01A4\x07I\x02\x02\u01A4\u01A5\x07\v\x02\x02\u01A5\u01A7\x05" +
		"h5\x02\u01A6\u01A8\x05.\x18\x02\u01A7\u01A6\x03\x02\x02\x02\u01A7\u01A8" +
		"\x03\x02\x02\x02\u01A8\u01A9\x03\x02\x02\x02\u01A9\u01AA\x07\x04\x02\x02" +
		"\u01AA\u01AB\x07\x06\x02\x02\u01AB\u01AD\x052\x1A\x02\u01AC\u01AE\x05" +
		"0\x19\x02\u01AD\u01AC\x03\x02\x02\x02\u01AD\u01AE\x03\x02\x02\x02\u01AE" +
		"\u01AF\x03\x02\x02\x02\u01AF\u01B0\x07\x04\x02\x02\u01B0\u01B2\x03\x02" +
		"\x02\x02\u01B1\u0192\x03\x02\x02\x02\u01B1\u01A2\x03\x02\x02\x02\u01B2" +
		"-\x03\x02\x02\x02\u01B3\u01B4\x07\x06\x02\x02\u01B4\u01B5\x07K\x02\x02" +
		"\u01B5\u01B6\x07\v\x02\x02\u01B6\u01B7\x05h5\x02\u01B7\u01B8\x07\x04\x02" +
		"\x02\u01B8/\x03\x02\x02\x02\u01B9\u01BA\x07\x06\x02\x02\u01BA\u01BD\t" +
		"\x03\x02\x02\u01BB\u01BC\x07\x06\x02\x02\u01BC\u01BE\t\x03\x02\x02\u01BD" +
		"\u01BB\x03\x02\x02\x02\u01BD\u01BE\x03\x02\x02\x02\u01BE1\x03\x02\x02" +
		"\x02\u01BF\u01C6\x05h5\x02\u01C0\u01C1\x07J\x02\x02\u01C1\u01C2\x07\v" +
		"\x02\x02\u01C2\u01C3\x05h5\x02\u01C3\u01C4\x07\x04\x02\x02\u01C4\u01C6" +
		"\x03\x02\x02\x02\u01C5\u01BF\x03\x02\x02\x02\u01C5\u01C0\x03\x02\x02\x02";
	private static readonly _serializedATNSegment1: string =
		"\u01C63\x03\x02\x02\x02\u01C7\u01C8\b\x1B\x01\x02\u01C8\u01C9\x07\v\x02" +
		"\x02\u01C9\u01CA\x054\x1B\x02\u01CA\u01CB\x07\x04\x02\x02\u01CB\u01D9" +
		"\x03\x02\x02\x02\u01CC\u01CD\x05b2\x02\u01CD\u01CE\x07!\x02\x02\u01CE" +
		"\u01CF\x054\x1B\x0E\u01CF\u01D9\x03\x02\x02\x02\u01D0\u01D9\x05\"\x12" +
		"\x02\u01D1\u01D9\x05$\x13\x02\u01D2\u01D9\x05&\x14\x02\u01D3\u01D9\x05" +
		"z>\x02\u01D4\u01D9\x05p9\x02\u01D5\u01D9\x05b2\x02\u01D6\u01D9\x05x=\x02" +
		"\u01D7\u01D9\x07S\x02\x02\u01D8\u01C7\x03\x02\x02\x02\u01D8\u01CC\x03" +
		"\x02\x02\x02\u01D8\u01D0\x03\x02\x02\x02\u01D8\u01D1\x03\x02\x02\x02\u01D8" +
		"\u01D2\x03\x02\x02\x02\u01D8\u01D3\x03\x02\x02\x02\u01D8\u01D4\x03\x02" +
		"\x02\x02\u01D8\u01D5\x03\x02\x02\x02\u01D8\u01D6\x03\x02\x02\x02\u01D8" +
		"\u01D7\x03\x02\x02\x02\u01D9\u01E8\x03\x02\x02\x02\u01DA\u01DB\f\x0F\x02" +
		"\x02\u01DB\u01DC\x07\b\x02\x02\u01DC\u01E7\x054\x1B\x10\u01DD\u01DE\f" +
		"\r\x02\x02\u01DE\u01DF\x07\x1F\x02\x02\u01DF\u01E7\x054\x1B\x0E\u01E0" +
		"\u01E1\f\f\x02\x02\u01E1\u01E2\x077\x02\x02\u01E2\u01E7\x054\x1B\r\u01E3" +
		"\u01E4\f\v\x02\x02\u01E4\u01E5\x07\t\x02\x02\u01E5\u01E7\x054\x1B\f\u01E6" +
		"\u01DA\x03\x02\x02\x02\u01E6\u01DD\x03\x02\x02\x02\u01E6\u01E0\x03\x02" +
		"\x02\x02\u01E6\u01E3\x03\x02\x02\x02\u01E7\u01EA\x03\x02\x02\x02\u01E8" +
		"\u01E6\x03\x02\x02\x02\u01E8\u01E9\x03\x02\x02\x02\u01E95\x03\x02\x02" +
		"\x02\u01EA\u01E8\x03\x02\x02\x02\u01EB\u01EC\x070\x02\x02\u01EC7\x03\x02" +
		"\x02\x02\u01ED\u01EE\x056\x1C\x02\u01EE\u01F3\x05:\x1E\x02\u01EF\u01F0" +
		"\x07\x06\x02\x02\u01F0\u01F2\x05:\x1E\x02\u01F1\u01EF\x03\x02\x02\x02" +
		"\u01F2\u01F5\x03\x02\x02\x02\u01F3\u01F1\x03\x02\x02\x02\u01F3\u01F4\x03" +
		"\x02\x02\x02\u01F49\x03\x02\x02\x02\u01F5\u01F3\x03\x02\x02\x02\u01F6" +
		"\u01F7\x05b2\x02\u01F7\u01F8\x05\x10\t\x02\u01F8;\x03\x02\x02\x02\u01F9" +
		"\u01FA\x078\x02\x02\u01FA=\x03\x02\x02\x02\u01FB\u01FC\x05<\x1F\x02\u01FC" +
		"\u0201\x05@!\x02\u01FD\u01FE\x07\x06\x02\x02\u01FE\u0200\x05@!\x02\u01FF" +
		"\u01FD\x03\x02\x02\x02\u0200\u0203\x03\x02\x02\x02\u0201\u01FF\x03\x02" +
		"\x02\x02\u0201\u0202\x03\x02\x02\x02\u0202?\x03\x02\x02\x02\u0203\u0201" +
		"\x03\x02\x02\x02\u0204\u0206\x05h5\x02\u0205\u0207\x05B\"\x02\u0206\u0205" +
		"\x03\x02\x02\x02\u0206\u0207\x03\x02\x02\x02\u0207\u0209\x03\x02\x02\x02" +
		"\u0208\u020A\x05F$\x02\u0209\u0208\x03\x02\x02\x02\u0209\u020A\x03\x02" +
		"\x02\x02\u020A\u020C\x03\x02\x02\x02\u020B\u020D\x05H%\x02\u020C\u020B" +
		"\x03\x02\x02\x02\u020C\u020D\x03\x02\x02\x02\u020DA\x03\x02\x02\x02\u020E" +
		"\u020F\x07 \x02\x02\u020F\u0210\x05D#\x02\u0210C\x03\x02\x02\x02\u0211" +
		"\u0212\t\x04\x02\x02\u0212E\x03\x02\x02\x02\u0213\u0214\x07>\x02\x02\u0214" +
		"G\x03\x02\x02\x02\u0215\u0216\t\x05\x02\x02\u0216I\x03\x02\x02\x02\u0217" +
		"\u0218\x07<\x02\x02\u0218K\x03\x02\x02\x02\u0219\u021A\x05J&\x02\u021A" +
		"\u021B\x07\"\x02\x02\u021B\u021D\x07\x19\x02\x02\u021C\u021E\x05`1\x02" +
		"\u021D\u021C\x03\x02\x02\x02\u021D\u021E\x03\x02\x02\x02\u021E\u023A\x03" +
		"\x02\x02\x02\u021F\u0221\x05J&\x02\u0220\u0222\x07\"\x02\x02\u0221\u0220" +
		"\x03\x02\x02\x02\u0221\u0222\x03\x02\x02\x02\u0222\u0223\x03\x02\x02\x02" +
		"\u0223\u0228\x05N(\x02\u0224\u0225\x07\x06\x02\x02\u0225\u0227\x05N(\x02" +
		"\u0226\u0224\x03\x02\x02\x02\u0227\u022A\x03\x02\x02\x02\u0228\u0226\x03" +
		"\x02\x02\x02\u0228\u0229\x03\x02\x02\x02\u0229\u022C\x03\x02\x02\x02\u022A" +
		"\u0228\x03\x02\x02\x02\u022B\u022D\x05`1\x02\u022C\u022B\x03\x02\x02\x02" +
		"\u022C\u022D\x03\x02\x02\x02\u022D\u023A\x03\x02\x02\x02\u022E\u0232\x07" +
		"=\x02\x02\u022F\u0231\x05R*\x02\u0230\u022F\x03\x02\x02\x02\u0231\u0234" +
		"\x03\x02\x02\x02\u0232\u0230\x03\x02\x02\x02\u0232\u0233\x03\x02\x02\x02" +
		"\u0233\u0235\x03\x02\x02\x02\u0234\u0232\x03\x02\x02\x02\u0235\u0237\x07" +
		"\x7F\x02\x02\u0236\u0238\x05`1\x02\u0237\u0236\x03\x02\x02\x02\u0237\u0238" +
		"\x03\x02\x02\x02\u0238\u023A\x03\x02\x02\x02\u0239\u0219\x03\x02\x02\x02" +
		"\u0239\u021F\x03\x02\x02\x02\u0239\u022E\x03\x02\x02\x02\u023AM\x03\x02" +
		"\x02\x02\u023B\u023F\x05h5\x02\u023C\u023F\x05&\x14\x02\u023D\u023F\x05" +
		"~@\x02\u023E\u023B\x03\x02\x02\x02\u023E\u023C\x03\x02\x02\x02\u023E\u023D" +
		"\x03\x02\x02\x02\u023F\u0241\x03\x02\x02\x02\u0240\u0242\x05T+\x02\u0241" +
		"\u0240\x03\x02\x02\x02\u0241\u0242\x03\x02\x02\x02\u0242O\x03\x02\x02" +
		"\x02\u0243\u0244\x07Q\x02\x02\u0244\u0245\x07\x81\x02\x02\u0245\u0247" +
		"\x07\x82\x02\x02\u0246\u0248\x07\x81\x02\x02\u0247\u0246\x03\x02\x02\x02" +
		"\u0247\u0248\x03\x02\x02\x02\u0248\u024D\x03\x02\x02\x02\u0249\u024A\x07" +
		"\x85\x02\x02\u024A\u024C\x07\x81\x02\x02\u024B\u0249\x03\x02\x02\x02\u024C" +
		"\u024F\x03\x02\x02\x02\u024D\u024B\x03\x02\x02\x02\u024D\u024E\x03\x02" +
		"\x02\x02\u024E\u0250\x03\x02\x02\x02\u024F\u024D\x03\x02\x02\x02\u0250" +
		"\u0251\x07\x83\x02\x02\u0251\u0255\x07\x84\x02\x02\u0252\u0254\x05R*\x02" +
		"\u0253\u0252\x03\x02\x02\x02\u0254\u0257\x03\x02\x02\x02\u0255\u0253\x03" +
		"\x02\x02\x02\u0255\u0256\x03\x02\x02\x02\u0256\u0258\x03\x02\x02\x02\u0257" +
		"\u0255\x03\x02\x02\x02\u0258\u0259\x07\x7F\x02\x02\u0259Q\x03\x02\x02" +
		"\x02\u025A\u025E\x07~\x02\x02\u025B\u025D\x05R*\x02\u025C\u025B\x03\x02" +
		"\x02\x02\u025D\u0260\x03\x02\x02\x02\u025E\u025C\x03\x02\x02\x02\u025E" +
		"\u025F\x03\x02\x02\x02\u025F\u0261\x03\x02\x02\x02\u0260\u025E\x03\x02" +
		"\x02\x02\u0261\u0262\x07\x7F\x02\x02\u0262S\x03\x02\x02\x02\u0263\u0264" +
		"\x07 \x02\x02\u0264\u0266\x05V,\x02\u0265\u0267\x05Z.\x02\u0266\u0265" +
		"\x03\x02\x02\x02\u0266\u0267\x03\x02\x02\x02\u0267U\x03\x02\x02\x02\u0268" +
		"\u026C\x07V\x02\x02\u0269\u026C\x05t;\x02\u026A\u026C\x05\xC4c\x02\u026B" +
		"\u0268\x03\x02\x02\x02\u026B\u0269\x03\x02\x02\x02\u026B\u026A\x03\x02" +
		"\x02\x02\u026CW\x03\x02\x02\x02\u026D\u026E\x073\x02\x02\u026E\u027C\x07" +
		"\x07\x02\x02\u026F\u0272\x07V\x02\x02\u0270\u0272\x05\xC4c\x02\u0271\u026F" +
		"\x03\x02\x02\x02\u0271\u0270\x03\x02\x02\x02\u0272\u0274\x03\x02\x02\x02" +
		"\u0273\u0275\x05Z.\x02\u0274\u0273\x03\x02\x02\x02\u0274\u0275\x03\x02" +
		"\x02\x02\u0275\u0276\x03\x02\x02\x02\u0276\u0278\x07\x07\x02\x02\u0277" +
		"\u0271\x03\x02\x02\x02\u0278\u0279\x03\x02\x02\x02\u0279\u0277\x03\x02" +
		"\x02\x02\u0279\u027A\x03\x02\x02\x02\u027A\u027C\x03\x02\x02\x02\u027B" +
		"\u026D\x03\x02\x02\x02\u027B\u0277\x03\x02\x02\x02\u027CY\x03\x02\x02" +
		"\x02\u027D\u027E\x07\f\x02\x02\u027E\u027F\x07\x05\x02\x02\u027F[\x03" +
		"\x02\x02\x02\u0280\u0281\x07,\x02\x02\u0281]\x03\x02\x02\x02\u0282\u0285" +
		"\x05\\/\x02\u0283\u0286\x05\x80A\x02\u0284\u0286\x05h5\x02\u0285\u0283" +
		"\x03\x02\x02\x02\u0285\u0284\x03\x02\x02\x02\u0286\u028E\x03\x02\x02\x02" +
		"\u0287\u028A\x07\x06\x02\x02\u0288\u028B\x05h5\x02\u0289\u028B\x05\x80" +
		"A\x02\u028A\u0288\x03\x02\x02\x02\u028A\u0289\x03\x02\x02\x02\u028B\u028D" +
		"\x03\x02\x02\x02\u028C\u0287\x03\x02\x02\x02\u028D\u0290\x03\x02\x02\x02" +
		"\u028E\u028C\x03\x02\x02\x02\u028E\u028F\x03\x02\x02\x02\u028F_\x03\x02" +
		"\x02\x02\u0290\u028E\x03\x02\x02\x02\u0291\u0292\x07L\x02\x02\u0292\u0295" +
		"\x05b2\x02\u0293\u0294\t\x06\x02\x02\u0294\u0296\x05b2\x02\u0295\u0293" +
		"\x03\x02\x02\x02\u0295\u0296\x03\x02\x02\x02\u0296\u0299\x03\x02\x02\x02" +
		"\u0297\u0298\x07O\x02\x02\u0298\u029A\x05b2\x02\u0299\u0297\x03\x02\x02" +
		"\x02\u0299\u029A\x03\x02\x02\x02\u029A\u029E\x03\x02\x02\x02\u029B\u029C" +
		"\x07O\x02\x02\u029C\u029E\x05b2\x02\u029D\u0291\x03\x02\x02\x02\u029D" +
		"\u029B\x03\x02\x02\x02\u029Ea\x03\x02\x02\x02\u029F\u02A0\x05d3\x02\u02A0" +
		"\u02A1\x07\x07\x02\x02\u02A1\u02A2\x05b2\x02\u02A2\u02A5\x03\x02\x02\x02" +
		"\u02A3\u02A5\x05d3\x02\u02A4\u029F\x03\x02\x02\x02\u02A4\u02A3\x03\x02" +
		"\x02\x02\u02A5c\x03\x02\x02\x02\u02A6\u02A9\x05j6\x02\u02A7\u02A9\x05" +
		"f4\x02\u02A8\u02A6\x03\x02\x02\x02\u02A8\u02A7\x03\x02\x02\x02\u02A9e" +
		"\x03\x02\x02\x02\u02AA\u02B3\x07S\x02\x02\u02AB\u02B3\x07V\x02\x02\u02AC" +
		"\u02B3\x05z>\x02\u02AD\u02B3\x05\xC4c\x02\u02AE\u02AF\x07*\x02\x02\u02AF" +
		"\u02B0\x07\v\x02\x02\u02B0\u02B3\x07\x04\x02\x02\u02B1\u02B3\x05x=\x02" +
		"\u02B2\u02AA\x03\x02\x02\x02\u02B2\u02AB\x03\x02\x02\x02\u02B2\u02AC\x03" +
		"\x02\x02\x02\u02B2\u02AD\x03\x02\x02\x02\u02B2\u02AE\x03\x02\x02\x02\u02B2" +
		"\u02B1\x03\x02\x02\x02\u02B3\u02B5\x03\x02\x02\x02\u02B4\u02B6\x05Z.\x02" +
		"\u02B5\u02B4\x03\x02\x02\x02\u02B5\u02B6\x03\x02\x02\x02\u02B6g\x03\x02" +
		"\x02\x02\u02B7\u02B9\x07\x15\x02\x02\u02B8\u02B7\x03\x02\x02\x02\u02B8" +
		"\u02B9\x03\x02\x02\x02\u02B9\u02BC\x03\x02\x02\x02\u02BA\u02BD\x05p9\x02" +
		"\u02BB\u02BD\x05b2\x02\u02BC\u02BA\x03\x02\x02\x02\u02BC\u02BB\x03\x02" +
		"\x02\x02\u02BDi\x03\x02\x02\x02\u02BE\u02BF\x07\x15\x02\x02\u02BF\u02C0" +
		"\x07V\x02\x02\u02C0k\x03\x02\x02\x02\u02C1\u02C3\x05n8\x02\u02C2\u02C4" +
		"\x05T+\x02\u02C3\u02C2\x03\x02\x02\x02\u02C3\u02C4\x03\x02\x02\x02\u02C4" +
		"m\x03\x02\x02\x02\u02C5\u02C8\x05b2\x02\u02C6\u02C8\x05p9\x02\u02C7\u02C5" +
		"\x03\x02\x02\x02\u02C7\u02C6\x03\x02\x02\x02\u02C8o\x03\x02\x02\x02\u02C9" +
		"\u02CA\x05b2\x02\u02CA\u02CC\x07\v\x02\x02\u02CB\u02CD\x05r:\x02\u02CC" +
		"\u02CB\x03\x02\x02\x02\u02CC\u02CD\x03\x02\x02\x02\u02CD\u02CE\x03\x02" +
		"\x02\x02\u02CE\u02CF\x07\x04\x02\x02\u02CFq\x03\x02\x02\x02\u02D0\u02D5" +
		"\x05h5\x02\u02D1\u02D2\x07\x06\x02\x02\u02D2\u02D4\x05h5\x02\u02D3\u02D1" +
		"\x03\x02\x02\x02\u02D4\u02D7\x03\x02\x02\x02\u02D5\u02D3\x03\x02\x02\x02" +
		"\u02D5\u02D6\x03\x02\x02\x02\u02D6s\x03\x02\x02\x02\u02D7\u02D5\x03\x02" +
		"\x02\x02\u02D8\u02D9\t\x07\x02\x02\u02D9u\x03\x02\x02\x02\u02DA\u02DB" +
		"\t\b\x02\x02\u02DBw\x03\x02\x02\x02\u02DC\u02DF\x05t;\x02\u02DD\u02DF" +
		"\x05v<\x02\u02DE\u02DC\x03\x02\x02\x02\u02DE\u02DD\x03\x02\x02\x02\u02DF" +
		"y\x03\x02\x02\x02\u02E0\u02E3\x07\f\x02\x02\u02E1\u02E4\x076\x02\x02\u02E2" +
		"\u02E4\x05|?\x02\u02E3\u02E1\x03\x02\x02\x02\u02E3\u02E2\x03\x02\x02\x02" +
		"\u02E4\u02E5\x03\x02\x02\x02\u02E5\u02E8\x07@\x02\x02\u02E6\u02E9\x07" +
		"6\x02\x02\u02E7\u02E9\x05|?\x02\u02E8\u02E6\x03\x02\x02\x02\u02E8\u02E7" +
		"\x03\x02\x02\x02\u02E9\u02EA\x03\x02\x02\x02\u02EA\u02EB\x07\x05\x02\x02" +
		"\u02EB{\x03\x02\x02\x02\u02EC\u02ED\x07V\x02\x02\u02ED\u02EE\x07\x07\x02" +
		"\x02\u02EE\u02EF\x07S\x02\x02\u02EF}\x03\x02\x02\x02\u02F0\u02F1\x07P" +
		"\x02\x02\u02F1\u02F2\x05\x86D\x02\u02F2\u02F3\x07\\\x02\x02\u02F3\x7F" +
		"\x03\x02\x02\x02\u02F4\u02F5\x07P\x02\x02\u02F5\u02F8\x05\x9CO\x02\u02F6" +
		"\u02F7\x07`\x02\x02\u02F7\u02F9\x05\x82B\x02\u02F8\u02F6\x03\x02\x02\x02" +
		"\u02F8\u02F9\x03\x02\x02\x02\u02F9\u02FC\x03\x02\x02\x02\u02FA\u02FB\x07" +
		"`\x02\x02\u02FB\u02FD\x05\x82B\x02\u02FC\u02FA\x03\x02\x02\x02\u02FC\u02FD" +
		"\x03\x02\x02\x02\u02FD\u02FE\x03\x02\x02\x02\u02FE\u02FF\x07\\\x02\x02" +
		"\u02FF\x81\x03\x02\x02\x02\u0300\u0303\x05\x9CO\x02\u0301\u0303\x05\x84" +
		"C\x02\u0302\u0300\x03\x02\x02\x02\u0302\u0301\x03\x02\x02\x02\u0303\x83" +
		"\x03\x02\x02\x02\u0304\u0305\t\t\x02\x02\u0305\u0306\x07[\x02\x02\u0306" +
		"\u0309\x07u\x02\x02\u0307\u0308\x07`\x02\x02\u0308\u030A\x07v\x02\x02" +
		"\u0309\u0307\x03\x02\x02\x02\u0309\u030A\x03\x02\x02\x02\u030A\u030B\x03" +
		"\x02\x02\x02\u030B\u030C\x07\\\x02\x02\u030C\x85\x03\x02\x02\x02\u030D" +
		"\u030F\x05\x92J\x02\u030E\u0310\x05\x8CG\x02\u030F\u030E\x03\x02\x02\x02" +
		"\u030F\u0310\x03\x02\x02\x02\u0310\u0312\x03\x02\x02\x02\u0311\u0313\x05" +
		"\x8EH\x02\u0312\u0311\x03\x02\x02\x02\u0312\u0313\x03\x02\x02\x02\u0313" +
		"\u0315\x03\x02\x02\x02\u0314\u0316\x05\x94K\x02\u0315\u0314\x03\x02\x02" +
		"\x02\u0315\u0316\x03\x02\x02\x02\u0316\u0318\x03\x02\x02\x02\u0317\u0319" +
		"\x05\xA4S\x02\u0318\u0317\x03\x02\x02\x02\u0318\u0319\x03\x02\x02\x02" +
		"\u0319\u031B\x03\x02\x02\x02\u031A\u031C\x05\xA6T\x02\u031B\u031A\x03" +
		"\x02\x02\x02\u031B\u031C\x03\x02\x02\x02\u031C\u031E\x03\x02\x02\x02\u031D" +
		"\u031F\x05\xA8U\x02\u031E\u031D\x03\x02\x02\x02\u031E\u031F\x03\x02\x02" +
		"\x02\u031F\u0321\x03\x02\x02\x02\u0320\u0322\x05\x88E\x02\u0321\u0320" +
		"\x03\x02\x02\x02\u0321\u0322\x03\x02\x02\x02\u0322\x87\x03\x02\x02\x02" +
		"\u0323\u0324\x07t\x02\x02\u0324\u0325\x07v\x02\x02\u0325\x89\x03\x02\x02" +
		"\x02\u0326\u0327\x07R\x02\x02\u0327\u0328\x07x\x02\x02\u0328\u0329\x07" +
		"[\x02\x02\u0329\u032E\x07x\x02\x02\u032A\u032B\x07`\x02\x02\u032B\u032D" +
		"\x07x\x02\x02\u032C\u032A\x03\x02\x02\x02\u032D\u0330\x03\x02\x02\x02" +
		"\u032E\u032C\x03\x02\x02\x02\u032E\u032F\x03\x02\x02\x02\u032F\u0331\x03" +
		"\x02\x02\x02\u0330\u032E\x03\x02\x02\x02\u0331\u0332\x07\\\x02\x02\u0332" +
		"\u0333\x07Y\x02\x02\u0333\u0334\x05\x86D\x02\u0334\u0335\x07Z\x02\x02" +
		"\u0335\x8B\x03\x02\x02\x02\u0336\u033D\x05\x98M\x02\u0337\u0338\x05\x9E" +
		"P\x02\u0338\u0339\x05\xA0Q\x02\u0339\u033D\x03\x02\x02\x02\u033A\u033D" +
		"\x05\x9EP\x02\u033B\u033D\x05\xA0Q\x02\u033C\u0336\x03\x02\x02\x02\u033C" +
		"\u0337\x03\x02\x02\x02\u033C\u033A\x03\x02\x02\x02\u033C\u033B\x03\x02" +
		"\x02\x02\u033D\x8D\x03\x02\x02\x02\u033E\u033F\x07r\x02\x02\u033F\u0340" +
		"\x05\x90I\x02\u0340\x8F\x03\x02\x02\x02\u0341\u0342\x07f\x02\x02\u0342" +
		"\u0343\x05\x9CO\x02\u0343\x91\x03\x02\x02\x02\u0344\u0345\x07h\x02\x02" +
		"\u0345\u0346\x05\xA2R\x02\u0346\x93\x03\x02\x02\x02\u0347\u0348\x07i\x02" +
		"\x02\u0348\u0349\x05\x96L\x02\u0349\x95\x03\x02\x02\x02\u034A\u034B\b" +
		"L\x01\x02\u034B\u034C\x07[\x02\x02\u034C\u034D\x05\x96L\x02\u034D\u034E" +
		"\x07\\\x02\x02\u034E\u0357\x03\x02\x02\x02\u034F\u0350\x07d\x02\x02\u0350" +
		"\u0352\x07g\x02\x02\u0351\u0353\x07e\x02\x02\u0352\u0351\x03\x02\x02\x02" +
		"\u0352\u0353\x03\x02\x02\x02\u0353\u0354\x03\x02\x02\x02\u0354\u0357\x05" +
		"\x96L\x04\u0355\u0357\x05\x9CO\x02\u0356\u034A\x03\x02\x02\x02\u0356\u034F" +
		"\x03\x02\x02\x02\u0356\u0355\x03\x02\x02\x02\u0357\u0361\x03\x02\x02\x02" +
		"\u0358\u0359\f\x07\x02\x02\u0359\u035A\x07b\x02\x02\u035A\u0360\x05\x96" +
		"L\b\u035B\u035C\f\x06\x02\x02\u035C\u035D\x05\x9AN\x02\u035D\u035E\x05" +
		"\x96L\x07\u035E\u0360\x03\x02\x02\x02\u035F\u0358\x03\x02\x02\x02\u035F" +
		"\u035B\x03\x02\x02\x02\u0360\u0363\x03\x02\x02\x02\u0361\u035F\x03\x02" +
		"\x02\x02\u0361\u0362\x03\x02\x02\x02\u0362\x97\x03\x02\x02\x02\u0363\u0361" +
		"\x03\x02\x02\x02\u0364\u0365\x07k\x02\x02\u0365\u0366\x05\x9CO\x02\u0366" +
		"\u0367\x07g\x02\x02\u0367\u0368\x05\x9CO\x02\u0368\x99\x03\x02\x02\x02" +
		"\u0369\u036A\t\n\x02\x02\u036A\x9B\x03\x02\x02\x02\u036B\u036F\x07a\x02" +
		"\x02\u036C\u0370\x07x\x02\x02\u036D\u0370\x07u\x02\x02\u036E\u0370\x05" +
		"\xACW\x02\u036F\u036C\x03\x02\x02\x02\u036F\u036D\x03\x02\x02\x02\u036F" +
		"\u036E\x03\x02\x02\x02\u0370\u0383\x03\x02\x02\x02\u0371\u0375\t\v\x02" +
		"\x02\u0372\u0373\x07]\x02\x02\u0373\u0374\x07u\x02\x02\u0374\u0376\x07" +
		"^\x02\x02\u0375\u0372\x03\x02\x02\x02\u0375\u0376\x03\x02\x02\x02\u0376" +
		"\u037F\x03\x02\x02\x02\u0377\u037B\x07_\x02\x02\u0378\u037C\x07x\x02\x02" +
		"\u0379\u037C\x07v\x02\x02\u037A\u037C\x05\xACW\x02\u037B\u0378\x03\x02" +
		"\x02\x02\u037B\u0379\x03\x02\x02\x02\u037B\u037A\x03\x02\x02\x02\u037C" +
		"\u037E\x03\x02\x02\x02\u037D\u0377\x03\x02\x02\x02\u037E\u0381\x03\x02" +
		"\x02\x02\u037F\u037D\x03\x02\x02\x02\u037F\u0380\x03\x02\x02\x02\u0380" +
		"\u0383\x03\x02\x02\x02\u0381\u037F\x03\x02\x02\x02\u0382\u036B\x03\x02" +
		"\x02\x02\u0382\u0371\x03\x02\x02\x02\u0383\x9D\x03\x02\x02\x02\u0384\u0385" +
		"\x07l\x02\x02\u0385\u0386\x07u\x02\x02\u0386\u0387\x07o\x02\x02\u0387" +
		"\x9F\x03\x02\x02\x02\u0388\u0389\x07m\x02\x02\u0389\u038A\x07u\x02\x02" +
		"\u038A\u038B\x07o\x02\x02\u038B\xA1\x03\x02\x02\x02\u038C\u0390\x07x\x02" +
		"\x02\u038D\u0390\x07v\x02\x02\u038E\u0390\x05\xACW\x02\u038F\u038C\x03" +
		"\x02\x02\x02\u038F\u038D\x03\x02\x02\x02\u038F\u038E\x03\x02\x02\x02\u0390" +
		"\u0393\x03\x02\x02\x02\u0391\u0392\x07_\x02\x02\u0392\u0394\x05\xA2R\x02" +
		"\u0393\u0391\x03\x02\x02\x02\u0393\u0394\x03\x02\x02\x02\u0394\xA3\x03" +
		"\x02\x02\x02\u0395\u0396\x07j\x02\x02\u0396\u039B\x05\xA2R\x02\u0397\u0398" +
		"\x07`\x02\x02\u0398\u039A\x05\xA2R\x02\u0399\u0397\x03\x02\x02\x02\u039A" +
		"\u039D\x03\x02\x02\x02\u039B\u0399\x03\x02\x02\x02\u039B\u039C\x03\x02" +
		"\x02\x02\u039C\u039F\x03\x02\x02\x02\u039D\u039B\x03\x02\x02\x02\u039E" +
		"\u03A0\x07n\x02\x02\u039F\u039E\x03\x02\x02\x02\u039F\u03A0\x03\x02\x02" +
		"\x02\u03A0\xA5\x03\x02\x02\x02\u03A1\u03A2\x07q\x02\x02\u03A2\u03A7\x05" +
		"\xAAV\x02\u03A3\u03A4\x07`\x02\x02\u03A4\u03A6\x05\xAAV\x02\u03A5\u03A3" +
		"\x03\x02\x02\x02\u03A6\u03A9\x03\x02\x02\x02\u03A7\u03A5\x03\x02\x02\x02" +
		"\u03A7\u03A8\x03\x02\x02\x02\u03A8\xA7\x03\x02\x02\x02\u03A9\u03A7\x03" +
		"\x02\x02\x02\u03AA\u03AB\x07s\x02\x02\u03AB\u03AC\x07u\x02\x02\u03AC\xA9" +
		"\x03\x02\x02\x02\u03AD\u03B0\x07X\x02\x02\u03AE\u03B0\x05\x9CO\x02\u03AF" +
		"\u03AD\x03\x02\x02\x02\u03AF\u03AE\x03\x02\x02\x02\u03B0\xAB\x03\x02\x02" +
		"\x02\u03B1\u03B2\t\f\x02\x02\u03B2\xAD\x03\x02\x02\x02\u03B3\u03B7\x07" +
		"z\x02\x02\u03B4\u03B6\x05\xAEX\x02\u03B5\u03B4\x03\x02\x02\x02\u03B6\u03B9" +
		"\x03\x02\x02\x02\u03B7\u03B5\x03\x02\x02\x02\u03B7\u03B8\x03\x02\x02\x02" +
		"\u03B8\u03BA\x03\x02\x02\x02\u03B9\u03B7\x03\x02\x02\x02\u03BA\u03BB\x07" +
		"{\x02\x02\u03BB\xAF\x03\x02\x02\x02\u03BC\u03BD\x05\xB4[\x02\u03BD\u03BE" +
		"\x05\xB2Z\x02\u03BE\xB1\x03\x02\x02\x02\u03BF\u03C0\bZ\x01\x02\u03C0\u03C1" +
		"\x07\v\x02\x02\u03C1\u03C2\x05\xB2Z\x02\u03C2\u03C3\x07\x04\x02\x02\u03C3" +
		"\u03D4\x03\x02\x02\x02\u03C4\u03C5\x05 \x11\x02\u03C5\u03C6\x07\b\x02" +
		"\x02\u03C6\u03C7\x05 \x11\x02\u03C7\u03D4\x03\x02\x02\x02\u03C8\u03C9" +
		"\x05 \x11\x02\u03C9\u03CA\x07\t\x02\x02\u03CA\u03CB\x05 \x11\x02\u03CB" +
		"\u03D4\x03\x02\x02\x02\u03CC\u03D4\x05p9\x02\u03CD\u03CE\x07A\x02\x02" +
		"\u03CE\u03D0\x07\x1F\x02\x02\u03CF\u03D1\x075\x02\x02\u03D0\u03CF\x03" +
		"\x02\x02\x02\u03D0\u03D1\x03\x02\x02\x02\u03D1\u03D2\x03\x02\x02\x02\u03D2" +
		"\u03D4\x05\x1C\x0F\x02\u03D3\u03BF\x03\x02\x02\x02\u03D3\u03C4\x03\x02" +
		"\x02\x02\u03D3\u03C8\x03\x02\x02\x02\u03D3\u03CC\x03\x02\x02\x02\u03D3" +
		"\u03CD\x03\x02\x02\x02\u03D4\u03DB\x03\x02\x02\x02\u03D5\u03D6\f\b\x02" +
		"\x02\u03D6\u03D7\x05\x1E\x10\x02\u03D7\u03D8\x05\xB2Z\t\u03D8\u03DA\x03" +
		"\x02\x02\x02\u03D9\u03D5\x03\x02\x02\x02\u03DA\u03DD\x03\x02\x02\x02\u03DB" +
		"\u03D9\x03\x02\x02\x02\u03DB\u03DC\x03\x02\x02\x02\u03DC\xB3\x03\x02\x02" +
		"\x02\u03DD\u03DB\x03\x02\x02\x02\u03DE\u03DF\x07N\x02\x02\u03DF\xB5\x03" +
		"\x02\x02\x02\u03E0\u03E1\x05h5\x02\u03E1\u03E2\x07\b\x02\x02\u03E2\u03E3" +
		"\x05\xB8]\x02\u03E3\xB7\x03\x02\x02\x02\u03E4\u03E7\x05\xC4c\x02\u03E5" +
		"\u03E7\x07S\x02\x02\u03E6\u03E4\x03\x02\x02\x02\u03E6\u03E5\x03\x02\x02" +
		"\x02\u03E7\xB9\x03\x02\x02\x02\u03E8\u03E9\x05\xBC_\x02\u03E9\xBB\x03" +
		"\x02\x02\x02\u03EA\u03EB\x07\n\x02\x02\u03EB\u03F0\x05\xBE`\x02\u03EC" +
		"\u03ED\x07\x06\x02\x02\u03ED\u03EF\x05\xBE`\x02\u03EE\u03EC\x03\x02\x02" +
		"\x02\u03EF\u03F2\x03\x02\x02\x02\u03F0\u03EE\x03\x02\x02\x02\u03F0\u03F1" +
		"\x03\x02\x02\x02\u03F1\u03F3\x03\x02\x02\x02\u03F2\u03F0\x03\x02\x02\x02" +
		"\u03F3\u03F4\x07\x03\x02\x02\u03F4\u03F8\x03\x02\x02\x02\u03F5\u03F6\x07" +
		"\n\x02\x02\u03F6\u03F8\x07\x03\x02\x02\u03F7\u03EA\x03\x02\x02\x02\u03F7" +
		"\u03F5\x03\x02\x02\x02\u03F8\xBD\x03\x02\x02\x02\u03F9\u03FA\x07T\x02" +
		"\x02\u03FA\u03FB\x07\x0E\x02\x02\u03FB\u03FC\x05\xC2b\x02\u03FC\xBF\x03" +
		"\x02\x02\x02\u03FD\u03FE\x07\f\x02\x02\u03FE\u0403\x05\xC2b\x02\u03FF" +
		"\u0400\x07\x06\x02\x02\u0400\u0402\x05\xC2b\x02\u0401\u03FF\x03\x02\x02" +
		"\x02\u0402\u0405\x03\x02\x02\x02\u0403\u0401\x03\x02\x02\x02\u0403\u0404" +
		"\x03\x02\x02\x02\u0404\u0406\x03\x02\x02\x02\u0405\u0403\x03\x02\x02\x02" +
		"\u0406\u0407\x07\x05\x02\x02\u0407\u040B\x03\x02\x02\x02\u0408\u0409\x07" +
		"\f\x02\x02\u0409\u040B\x07\x05\x02\x02\u040A\u03FD\x03\x02\x02\x02\u040A" +
		"\u0408\x03\x02\x02\x02\u040B\xC1\x03\x02\x02\x02\u040C\u0414\x07T\x02" +
		"\x02\u040D\u0414\x07S\x02\x02\u040E\u0414\x05\xBC_\x02\u040F\u0414\x05" +
		"\xC0a\x02\u0410\u0414\x07A\x02\x02\u0411\u0414\x07&\x02\x02\u0412\u0414" +
		"\x076\x02\x02\u0413\u040C\x03\x02\x02\x02\u0413\u040D\x03\x02\x02\x02" +
		"\u0413\u040E\x03\x02\x02\x02\u0413\u040F\x03\x02\x02\x02\u0413\u0410\x03" +
		"\x02\x02\x02\u0413\u0411\x03\x02\x02\x02\u0413\u0412\x03\x02\x02\x02\u0414" +
		"\xC3\x03\x02\x02\x02\u0415\u0416\t\r\x02\x02\u0416\xC5\x03\x02\x02\x02" +
		"}\xC9\xCF\xD4\xD7\xDA\xDD\xE0\xE3\xE6\xE9\xEC\xEF\xF5\xFC\u0108\u010F" +
		"\u0111\u0116\u011B\u011E\u0122\u012C\u014B\u014E\u0156\u015F\u0165\u016E" +
		"\u017D\u0182\u0186\u0189\u0199\u019E\u01A7\u01AD\u01B1\u01BD\u01C5\u01D8" +
		"\u01E6\u01E8\u01F3\u0201\u0206\u0209\u020C\u021D\u0221\u0228\u022C\u0232" +
		"\u0237\u0239\u023E\u0241\u0247\u024D\u0255\u025E\u0266\u026B\u0271\u0274" +
		"\u0279\u027B\u0285\u028A\u028E\u0295\u0299\u029D\u02A4\u02A8\u02B2\u02B5" +
		"\u02B8\u02BC\u02C3\u02C7\u02CC\u02D5\u02DE\u02E3\u02E8\u02F8\u02FC\u0302" +
		"\u0309\u030F\u0312\u0315\u0318\u031B\u031E\u0321\u032E\u033C\u0352\u0356" +
		"\u035F\u0361\u036F\u0375\u037B\u037F\u0382\u038F\u0393\u039B\u039F\u03A7" +
		"\u03AF\u03B7\u03D0\u03D3\u03DB\u03E6\u03F0\u03F7\u0403\u040A\u0413";
	public static readonly _serializedATN: string = Utils.join(
		[
			BaseRqlParser._serializedATNSegment0,
			BaseRqlParser._serializedATNSegment1,
		],
		"",
	);
	public static __ATN: ATN;
	public static get _ATN(): ATN {
		if (!BaseRqlParser.__ATN) {
			BaseRqlParser.__ATN = new ATNDeserializer().deserialize(Utils.toCharArray(BaseRqlParser._serializedATN));
		}

		return BaseRqlParser.__ATN;
	}

}

export class ProgContext extends ParserRuleContext {
	public fromStatement(): FromStatementContext {
		return this.getRuleContext(0, FromStatementContext);
	}
	public EOF(): TerminalNode { return this.getToken(BaseRqlParser.EOF, 0); }
	public parameterBeforeQuery(): ParameterBeforeQueryContext[];
	public parameterBeforeQuery(i: number): ParameterBeforeQueryContext;
	public parameterBeforeQuery(i?: number): ParameterBeforeQueryContext | ParameterBeforeQueryContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ParameterBeforeQueryContext);
		} else {
			return this.getRuleContext(i, ParameterBeforeQueryContext);
		}
	}
	public functionStatment(): FunctionStatmentContext[];
	public functionStatment(i: number): FunctionStatmentContext;
	public functionStatment(i?: number): FunctionStatmentContext | FunctionStatmentContext[] {
		if (i === undefined) {
			return this.getRuleContexts(FunctionStatmentContext);
		} else {
			return this.getRuleContext(i, FunctionStatmentContext);
		}
	}
	public groupByStatement(): GroupByStatementContext | undefined {
		return this.tryGetRuleContext(0, GroupByStatementContext);
	}
	public whereStatement(): WhereStatementContext | undefined {
		return this.tryGetRuleContext(0, WhereStatementContext);
	}
	public orderByStatement(): OrderByStatementContext | undefined {
		return this.tryGetRuleContext(0, OrderByStatementContext);
	}
	public loadStatement(): LoadStatementContext | undefined {
		return this.tryGetRuleContext(0, LoadStatementContext);
	}
	public updateStatement(): UpdateStatementContext | undefined {
		return this.tryGetRuleContext(0, UpdateStatementContext);
	}
	public filterStatement(): FilterStatementContext | undefined {
		return this.tryGetRuleContext(0, FilterStatementContext);
	}
	public selectStatement(): SelectStatementContext | undefined {
		return this.tryGetRuleContext(0, SelectStatementContext);
	}
	public includeStatement(): IncludeStatementContext | undefined {
		return this.tryGetRuleContext(0, IncludeStatementContext);
	}
	public limitStatement(): LimitStatementContext | undefined {
		return this.tryGetRuleContext(0, LimitStatementContext);
	}
	public json(): JsonContext | undefined {
		return this.tryGetRuleContext(0, JsonContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_prog; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitProg) {
			return visitor.visitProg(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FunctionStatmentContext extends ParserRuleContext {
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_functionStatment; }
	public copyFrom(ctx: FunctionStatmentContext): void {
		super.copyFrom(ctx);
	}
}
export class JavaScriptFunctionContext extends FunctionStatmentContext {
	public jsFunction(): JsFunctionContext {
		return this.getRuleContext(0, JsFunctionContext);
	}
	constructor(ctx: FunctionStatmentContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitJavaScriptFunction) {
			return visitor.visitJavaScriptFunction(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class TimeSeriesFunctionContext extends FunctionStatmentContext {
	public tsFunction(): TsFunctionContext {
		return this.getRuleContext(0, TsFunctionContext);
	}
	constructor(ctx: FunctionStatmentContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTimeSeriesFunction) {
			return visitor.visitTimeSeriesFunction(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class UpdateStatementContext extends ParserRuleContext {
	public UPDATE(): TerminalNode { return this.getToken(BaseRqlParser.UPDATE, 0); }
	public US_OP(): TerminalNode { return this.getToken(BaseRqlParser.US_OP, 0); }
	public US_CL(): TerminalNode { return this.getToken(BaseRqlParser.US_CL, 0); }
	public EOF(): TerminalNode { return this.getToken(BaseRqlParser.EOF, 0); }
	public updateBody(): UpdateBodyContext[];
	public updateBody(i: number): UpdateBodyContext;
	public updateBody(i?: number): UpdateBodyContext | UpdateBodyContext[] {
		if (i === undefined) {
			return this.getRuleContexts(UpdateBodyContext);
		} else {
			return this.getRuleContext(i, UpdateBodyContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_updateStatement; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitUpdateStatement) {
			return visitor.visitUpdateStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FromModeContext extends ParserRuleContext {
	public FROM(): TerminalNode { return this.getToken(BaseRqlParser.FROM, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_fromMode; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitFromMode) {
			return visitor.visitFromMode(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FromStatementContext extends ParserRuleContext {
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_fromStatement; }
	public copyFrom(ctx: FromStatementContext): void {
		super.copyFrom(ctx);
	}
}
export class CollectionByIndexContext extends FromStatementContext {
	public _collection!: IndexNameContext;
	public fromMode(): FromModeContext {
		return this.getRuleContext(0, FromModeContext);
	}
	public INDEX(): TerminalNode { return this.getToken(BaseRqlParser.INDEX, 0); }
	public indexName(): IndexNameContext {
		return this.getRuleContext(0, IndexNameContext);
	}
	public aliasWithOptionalAs(): AliasWithOptionalAsContext | undefined {
		return this.tryGetRuleContext(0, AliasWithOptionalAsContext);
	}
	constructor(ctx: FromStatementContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitCollectionByIndex) {
			return visitor.visitCollectionByIndex(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class AllCollectionsContext extends FromStatementContext {
	public FROM(): TerminalNode { return this.getToken(BaseRqlParser.FROM, 0); }
	public ALL_DOCS(): TerminalNode { return this.getToken(BaseRqlParser.ALL_DOCS, 0); }
	constructor(ctx: FromStatementContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitAllCollections) {
			return visitor.visitAllCollections(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class CollectionByNameContext extends FromStatementContext {
	public _collection!: CollectionNameContext;
	public fromMode(): FromModeContext {
		return this.getRuleContext(0, FromModeContext);
	}
	public collectionName(): CollectionNameContext {
		return this.getRuleContext(0, CollectionNameContext);
	}
	public aliasWithOptionalAs(): AliasWithOptionalAsContext | undefined {
		return this.tryGetRuleContext(0, AliasWithOptionalAsContext);
	}
	constructor(ctx: FromStatementContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitCollectionByName) {
			return visitor.visitCollectionByName(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class IndexNameContext extends ParserRuleContext {
	public WORD(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.WORD, 0); }
	public string(): StringContext | undefined {
		return this.tryGetRuleContext(0, StringContext);
	}
	public identifiersWithoutRootKeywords(): IdentifiersWithoutRootKeywordsContext | undefined {
		return this.tryGetRuleContext(0, IdentifiersWithoutRootKeywordsContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_indexName; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitIndexName) {
			return visitor.visitIndexName(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class CollectionNameContext extends ParserRuleContext {
	public WORD(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.WORD, 0); }
	public string(): StringContext | undefined {
		return this.tryGetRuleContext(0, StringContext);
	}
	public identifiersWithoutRootKeywords(): IdentifiersWithoutRootKeywordsContext | undefined {
		return this.tryGetRuleContext(0, IdentifiersWithoutRootKeywordsContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_collectionName; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitCollectionName) {
			return visitor.visitCollectionName(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class AliasWithOptionalAsContext extends ParserRuleContext {
	public _name!: AliasNameContext;
	public aliasName(): AliasNameContext {
		return this.getRuleContext(0, AliasNameContext);
	}
	public AS(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.AS, 0); }
	public asArray(): AsArrayContext | undefined {
		return this.tryGetRuleContext(0, AsArrayContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_aliasWithOptionalAs; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitAliasWithOptionalAs) {
			return visitor.visitAliasWithOptionalAs(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class GroupByModeContext extends ParserRuleContext {
	public GROUP_BY(): TerminalNode { return this.getToken(BaseRqlParser.GROUP_BY, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_groupByMode; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitGroupByMode) {
			return visitor.visitGroupByMode(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class GroupByStatementContext extends ParserRuleContext {
	public _value!: ParameterWithOptionalAliasContext;
	public groupByMode(): GroupByModeContext {
		return this.getRuleContext(0, GroupByModeContext);
	}
	public parameterWithOptionalAlias(): ParameterWithOptionalAliasContext[];
	public parameterWithOptionalAlias(i: number): ParameterWithOptionalAliasContext;
	public parameterWithOptionalAlias(i?: number): ParameterWithOptionalAliasContext | ParameterWithOptionalAliasContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ParameterWithOptionalAliasContext);
		} else {
			return this.getRuleContext(i, ParameterWithOptionalAliasContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_groupByStatement; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitGroupByStatement) {
			return visitor.visitGroupByStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SuggestGroupByContext extends ParserRuleContext {
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_suggestGroupBy; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitSuggestGroupBy) {
			return visitor.visitSuggestGroupBy(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class WhereModeContext extends ParserRuleContext {
	public WHERE(): TerminalNode { return this.getToken(BaseRqlParser.WHERE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_whereMode; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitWhereMode) {
			return visitor.visitWhereMode(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class WhereStatementContext extends ParserRuleContext {
	public whereMode(): WhereModeContext {
		return this.getRuleContext(0, WhereModeContext);
	}
	public expr(): ExprContext {
		return this.getRuleContext(0, ExprContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_whereStatement; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitWhereStatement) {
			return visitor.visitWhereStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ExprContext extends ParserRuleContext {
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_expr; }
	public copyFrom(ctx: ExprContext): void {
		super.copyFrom(ctx);
	}
}
export class BinaryExpressionContext extends ExprContext {
	public _left!: ExprContext;
	public _right!: ExprContext;
	public binary(): BinaryContext {
		return this.getRuleContext(0, BinaryContext);
	}
	public expr(): ExprContext[];
	public expr(i: number): ExprContext;
	public expr(i?: number): ExprContext | ExprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ExprContext);
		} else {
			return this.getRuleContext(i, ExprContext);
		}
	}
	constructor(ctx: ExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitBinaryExpression) {
			return visitor.visitBinaryExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class OpParContext extends ExprContext {
	public OP_PAR(): TerminalNode { return this.getToken(BaseRqlParser.OP_PAR, 0); }
	public expr(): ExprContext {
		return this.getRuleContext(0, ExprContext);
	}
	public CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.CL_PAR, 0); }
	constructor(ctx: ExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitOpPar) {
			return visitor.visitOpPar(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class EqualExpressionContext extends ExprContext {
	public _left!: ExprValueContext;
	public _right!: ExprValueContext;
	public EQUAL(): TerminalNode { return this.getToken(BaseRqlParser.EQUAL, 0); }
	public exprValue(): ExprValueContext[];
	public exprValue(i: number): ExprValueContext;
	public exprValue(i?: number): ExprValueContext | ExprValueContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ExprValueContext);
		} else {
			return this.getRuleContext(i, ExprValueContext);
		}
	}
	constructor(ctx: ExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitEqualExpression) {
			return visitor.visitEqualExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class MathExpressionContext extends ExprContext {
	public _left!: ExprValueContext;
	public _right!: ExprValueContext;
	public MATH(): TerminalNode { return this.getToken(BaseRqlParser.MATH, 0); }
	public exprValue(): ExprValueContext[];
	public exprValue(i: number): ExprValueContext;
	public exprValue(i?: number): ExprValueContext | ExprValueContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ExprValueContext);
		} else {
			return this.getRuleContext(i, ExprValueContext);
		}
	}
	constructor(ctx: ExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitMathExpression) {
			return visitor.visitMathExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class SpecialFunctionstContext extends ExprContext {
	public specialFunctions(): SpecialFunctionsContext {
		return this.getRuleContext(0, SpecialFunctionsContext);
	}
	constructor(ctx: ExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitSpecialFunctionst) {
			return visitor.visitSpecialFunctionst(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class InExprContext extends ExprContext {
	public inFunction(): InFunctionContext {
		return this.getRuleContext(0, InFunctionContext);
	}
	constructor(ctx: ExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitInExpr) {
			return visitor.visitInExpr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class VecExprContext extends ExprContext {
	public vectorSearch(): VectorSearchContext {
		return this.getRuleContext(0, VectorSearchContext);
	}
	constructor(ctx: ExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitVecExpr) {
			return visitor.visitVecExpr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class BetweenExprContext extends ExprContext {
	public betweenFunction(): BetweenFunctionContext {
		return this.getRuleContext(0, BetweenFunctionContext);
	}
	constructor(ctx: ExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitBetweenExpr) {
			return visitor.visitBetweenExpr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class NormalFuncContext extends ExprContext {
	public _funcExpr!: FunctionContext;
	public function(): FunctionContext {
		return this.getRuleContext(0, FunctionContext);
	}
	constructor(ctx: ExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitNormalFunc) {
			return visitor.visitNormalFunc(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class BooleanExpressionContext extends ExprContext {
	public TRUE(): TerminalNode { return this.getToken(BaseRqlParser.TRUE, 0); }
	public AND(): TerminalNode { return this.getToken(BaseRqlParser.AND, 0); }
	public expr(): ExprContext {
		return this.getRuleContext(0, ExprContext);
	}
	public NOT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NOT, 0); }
	constructor(ctx: ExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitBooleanExpression) {
			return visitor.visitBooleanExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class BinaryContext extends ParserRuleContext {
	public AND(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.AND, 0); }
	public NOT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NOT, 0); }
	public OR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.OR, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_binary; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitBinary) {
			return visitor.visitBinary(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ExprValueContext extends ParserRuleContext {
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_exprValue; }
	public copyFrom(ctx: ExprValueContext): void {
		super.copyFrom(ctx);
	}
}
export class ParameterExprContext extends ExprValueContext {
	public literal(): LiteralContext {
		return this.getRuleContext(0, LiteralContext);
	}
	constructor(ctx: ExprValueContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitParameterExpr) {
			return visitor.visitParameterExpr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class InFunctionContext extends ParserRuleContext {
	public _value!: LiteralContext;
	public _first!: LiteralContext;
	public _next!: LiteralContext;
	public IN(): TerminalNode { return this.getToken(BaseRqlParser.IN, 0); }
	public OP_PAR(): TerminalNode { return this.getToken(BaseRqlParser.OP_PAR, 0); }
	public CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.CL_PAR, 0); }
	public literal(): LiteralContext[];
	public literal(i: number): LiteralContext;
	public literal(i?: number): LiteralContext | LiteralContext[] {
		if (i === undefined) {
			return this.getRuleContexts(LiteralContext);
		} else {
			return this.getRuleContext(i, LiteralContext);
		}
	}
	public ALL(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.ALL, 0); }
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_inFunction; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitInFunction) {
			return visitor.visitInFunction(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class BetweenFunctionContext extends ParserRuleContext {
	public _value!: LiteralContext;
	public _from!: LiteralContext;
	public _to!: LiteralContext;
	public BETWEEN(): TerminalNode { return this.getToken(BaseRqlParser.BETWEEN, 0); }
	public AND(): TerminalNode { return this.getToken(BaseRqlParser.AND, 0); }
	public literal(): LiteralContext[];
	public literal(i: number): LiteralContext;
	public literal(i?: number): LiteralContext | LiteralContext[] {
		if (i === undefined) {
			return this.getRuleContexts(LiteralContext);
		} else {
			return this.getRuleContext(i, LiteralContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_betweenFunction; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitBetweenFunction) {
			return visitor.visitBetweenFunction(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SpecialFunctionsContext extends ParserRuleContext {
	public specialFunctionName(): SpecialFunctionNameContext {
		return this.getRuleContext(0, SpecialFunctionNameContext);
	}
	public OP_PAR(): TerminalNode { return this.getToken(BaseRqlParser.OP_PAR, 0); }
	public CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.CL_PAR, 0); }
	public specialParam(): SpecialParamContext[];
	public specialParam(i: number): SpecialParamContext;
	public specialParam(i?: number): SpecialParamContext | SpecialParamContext[] {
		if (i === undefined) {
			return this.getRuleContexts(SpecialParamContext);
		} else {
			return this.getRuleContext(i, SpecialParamContext);
		}
	}
	public aliasWithRequiredAs(): AliasWithRequiredAsContext[];
	public aliasWithRequiredAs(i: number): AliasWithRequiredAsContext;
	public aliasWithRequiredAs(i?: number): AliasWithRequiredAsContext | AliasWithRequiredAsContext[] {
		if (i === undefined) {
			return this.getRuleContexts(AliasWithRequiredAsContext);
		} else {
			return this.getRuleContext(i, AliasWithRequiredAsContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_specialFunctions; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitSpecialFunctions) {
			return visitor.visitSpecialFunctions(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SpecialFunctionNameContext extends ParserRuleContext {
	public ID(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.ID, 0); }
	public FUZZY(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.FUZZY, 0); }
	public SEARCH(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.SEARCH, 0); }
	public FACET(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.FACET, 0); }
	public BOOST(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.BOOST, 0); }
	public STARTS_WITH(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.STARTS_WITH, 0); }
	public ENDS_WITH(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.ENDS_WITH, 0); }
	public MORELIKETHIS(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.MORELIKETHIS, 0); }
	public INTERSECT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.INTERSECT, 0); }
	public EXACT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.EXACT, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_specialFunctionName; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitSpecialFunctionName) {
			return visitor.visitSpecialFunctionName(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class VectorSearchContext extends ParserRuleContext {
	public VECTOR_SEARCH(): TerminalNode { return this.getToken(BaseRqlParser.VECTOR_SEARCH, 0); }
	public vectorSearchParam(): VectorSearchParamContext {
		return this.getRuleContext(0, VectorSearchParamContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_vectorSearch; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitVectorSearch) {
			return visitor.visitVectorSearch(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class VectorSearchParamContext extends ParserRuleContext {
	public OP_PAR(): TerminalNode[];
	public OP_PAR(i: number): TerminalNode;
	public OP_PAR(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.OP_PAR);
		} else {
			return this.getToken(BaseRqlParser.OP_PAR, i);
		}
	}
	public COMMA(): TerminalNode { return this.getToken(BaseRqlParser.COMMA, 0); }
	public vectorSearchValue(): VectorSearchValueContext {
		return this.getRuleContext(0, VectorSearchValueContext);
	}
	public CL_PAR(): TerminalNode[];
	public CL_PAR(i: number): TerminalNode;
	public CL_PAR(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.CL_PAR);
		} else {
			return this.getToken(BaseRqlParser.CL_PAR, i);
		}
	}
	public literal(): LiteralContext | undefined {
		return this.tryGetRuleContext(0, LiteralContext);
	}
	public vectorSearchAdditionalParams(): VectorSearchAdditionalParamsContext | undefined {
		return this.tryGetRuleContext(0, VectorSearchAdditionalParamsContext);
	}
	public EMBEDDING_ARRAY(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.EMBEDDING_ARRAY, 0); }
	public variable(): VariableContext | undefined {
		return this.tryGetRuleContext(0, VariableContext);
	}
	public EMBEDDING_TEXT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.EMBEDDING_TEXT, 0); }
	public aiTaskMethod(): AiTaskMethodContext | undefined {
		return this.tryGetRuleContext(0, AiTaskMethodContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_vectorSearchParam; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitVectorSearchParam) {
			return visitor.visitVectorSearchParam(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class AiTaskMethodContext extends ParserRuleContext {
	public COMMA(): TerminalNode { return this.getToken(BaseRqlParser.COMMA, 0); }
	public AI_TASK(): TerminalNode { return this.getToken(BaseRqlParser.AI_TASK, 0); }
	public OP_PAR(): TerminalNode { return this.getToken(BaseRqlParser.OP_PAR, 0); }
	public literal(): LiteralContext {
		return this.getRuleContext(0, LiteralContext);
	}
	public CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.CL_PAR, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_aiTaskMethod; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitAiTaskMethod) {
			return visitor.visitAiTaskMethod(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class VectorSearchAdditionalParamsContext extends ParserRuleContext {
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	public NUM(): TerminalNode[];
	public NUM(i: number): TerminalNode;
	public NUM(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.NUM);
		} else {
			return this.getToken(BaseRqlParser.NUM, i);
		}
	}
	public NULL(): TerminalNode[];
	public NULL(i: number): TerminalNode;
	public NULL(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.NULL);
		} else {
			return this.getToken(BaseRqlParser.NULL, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_vectorSearchAdditionalParams; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitVectorSearchAdditionalParams) {
			return visitor.visitVectorSearchAdditionalParams(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class VectorSearchValueContext extends ParserRuleContext {
	public literal(): LiteralContext {
		return this.getRuleContext(0, LiteralContext);
	}
	public EMBEDDING_FOR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.EMBEDDING_FOR, 0); }
	public OP_PAR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.OP_PAR, 0); }
	public CL_PAR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.CL_PAR, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_vectorSearchValue; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitVectorSearchValue) {
			return visitor.visitVectorSearchValue(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SpecialParamContext extends ParserRuleContext {
	public OP_PAR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.OP_PAR, 0); }
	public specialParam(): SpecialParamContext[];
	public specialParam(i: number): SpecialParamContext;
	public specialParam(i?: number): SpecialParamContext | SpecialParamContext[] {
		if (i === undefined) {
			return this.getRuleContexts(SpecialParamContext);
		} else {
			return this.getRuleContext(i, SpecialParamContext);
		}
	}
	public CL_PAR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.CL_PAR, 0); }
	public EQUAL(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.EQUAL, 0); }
	public variable(): VariableContext | undefined {
		return this.tryGetRuleContext(0, VariableContext);
	}
	public BETWEEN(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.BETWEEN, 0); }
	public AND(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.AND, 0); }
	public OR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.OR, 0); }
	public MATH(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.MATH, 0); }
	public inFunction(): InFunctionContext | undefined {
		return this.tryGetRuleContext(0, InFunctionContext);
	}
	public betweenFunction(): BetweenFunctionContext | undefined {
		return this.tryGetRuleContext(0, BetweenFunctionContext);
	}
	public specialFunctions(): SpecialFunctionsContext | undefined {
		return this.tryGetRuleContext(0, SpecialFunctionsContext);
	}
	public date(): DateContext | undefined {
		return this.tryGetRuleContext(0, DateContext);
	}
	public function(): FunctionContext | undefined {
		return this.tryGetRuleContext(0, FunctionContext);
	}
	public identifiersAllNames(): IdentifiersAllNamesContext | undefined {
		return this.tryGetRuleContext(0, IdentifiersAllNamesContext);
	}
	public NUM(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NUM, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_specialParam; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitSpecialParam) {
			return visitor.visitSpecialParam(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class LoadModeContext extends ParserRuleContext {
	public LOAD(): TerminalNode { return this.getToken(BaseRqlParser.LOAD, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_loadMode; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitLoadMode) {
			return visitor.visitLoadMode(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class LoadStatementContext extends ParserRuleContext {
	public _item!: LoadDocumentByNameContext;
	public loadMode(): LoadModeContext {
		return this.getRuleContext(0, LoadModeContext);
	}
	public loadDocumentByName(): LoadDocumentByNameContext[];
	public loadDocumentByName(i: number): LoadDocumentByNameContext;
	public loadDocumentByName(i?: number): LoadDocumentByNameContext | LoadDocumentByNameContext[] {
		if (i === undefined) {
			return this.getRuleContexts(LoadDocumentByNameContext);
		} else {
			return this.getRuleContext(i, LoadDocumentByNameContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_loadStatement; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitLoadStatement) {
			return visitor.visitLoadStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class LoadDocumentByNameContext extends ParserRuleContext {
	public _name!: VariableContext;
	public _as!: AliasWithOptionalAsContext;
	public variable(): VariableContext {
		return this.getRuleContext(0, VariableContext);
	}
	public aliasWithOptionalAs(): AliasWithOptionalAsContext {
		return this.getRuleContext(0, AliasWithOptionalAsContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_loadDocumentByName; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitLoadDocumentByName) {
			return visitor.visitLoadDocumentByName(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class OrderByModeContext extends ParserRuleContext {
	public ORDER_BY(): TerminalNode { return this.getToken(BaseRqlParser.ORDER_BY, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_orderByMode; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitOrderByMode) {
			return visitor.visitOrderByMode(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class OrderByStatementContext extends ParserRuleContext {
	public _value!: OrderByItemContext;
	public orderByMode(): OrderByModeContext {
		return this.getRuleContext(0, OrderByModeContext);
	}
	public orderByItem(): OrderByItemContext[];
	public orderByItem(i: number): OrderByItemContext;
	public orderByItem(i?: number): OrderByItemContext | OrderByItemContext[] {
		if (i === undefined) {
			return this.getRuleContexts(OrderByItemContext);
		} else {
			return this.getRuleContext(i, OrderByItemContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_orderByStatement; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitOrderByStatement) {
			return visitor.visitOrderByStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class OrderByItemContext extends ParserRuleContext {
	public _value!: LiteralContext;
	public _order!: OrderBySortingContext;
	public _orderValueType!: OrderByOrderContext;
	public _nullsValue!: OrderByNullsContext;
	public literal(): LiteralContext {
		return this.getRuleContext(0, LiteralContext);
	}
	public orderBySorting(): OrderBySortingContext | undefined {
		return this.tryGetRuleContext(0, OrderBySortingContext);
	}
	public orderByOrder(): OrderByOrderContext | undefined {
		return this.tryGetRuleContext(0, OrderByOrderContext);
	}
	public orderByNulls(): OrderByNullsContext | undefined {
		return this.tryGetRuleContext(0, OrderByNullsContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_orderByItem; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitOrderByItem) {
			return visitor.visitOrderByItem(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class OrderBySortingContext extends ParserRuleContext {
	public _sortingMode!: OrderBySortingAsContext;
	public AS(): TerminalNode { return this.getToken(BaseRqlParser.AS, 0); }
	public orderBySortingAs(): OrderBySortingAsContext {
		return this.getRuleContext(0, OrderBySortingAsContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_orderBySorting; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitOrderBySorting) {
			return visitor.visitOrderBySorting(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class OrderBySortingAsContext extends ParserRuleContext {
	public STRING_W(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.STRING_W, 0); }
	public ALPHANUMERIC(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.ALPHANUMERIC, 0); }
	public LONG(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.LONG, 0); }
	public DOUBLE(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.DOUBLE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_orderBySortingAs; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitOrderBySortingAs) {
			return visitor.visitOrderBySortingAs(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class OrderByOrderContext extends ParserRuleContext {
	public SORTING(): TerminalNode { return this.getToken(BaseRqlParser.SORTING, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_orderByOrder; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitOrderByOrder) {
			return visitor.visitOrderByOrder(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class OrderByNullsContext extends ParserRuleContext {
	public NULLS_FIRST(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NULLS_FIRST, 0); }
	public NULLS_LAST(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NULLS_LAST, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_orderByNulls; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitOrderByNulls) {
			return visitor.visitOrderByNulls(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SelectModeContext extends ParserRuleContext {
	public SELECT(): TerminalNode { return this.getToken(BaseRqlParser.SELECT, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_selectMode; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitSelectMode) {
			return visitor.visitSelectMode(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SelectStatementContext extends ParserRuleContext {
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_selectStatement; }
	public copyFrom(ctx: SelectStatementContext): void {
		super.copyFrom(ctx);
	}
}
export class GetAllDistinctContext extends SelectStatementContext {
	public selectMode(): SelectModeContext {
		return this.getRuleContext(0, SelectModeContext);
	}
	public DISTINCT(): TerminalNode { return this.getToken(BaseRqlParser.DISTINCT, 0); }
	public STAR(): TerminalNode { return this.getToken(BaseRqlParser.STAR, 0); }
	public limitStatement(): LimitStatementContext | undefined {
		return this.tryGetRuleContext(0, LimitStatementContext);
	}
	constructor(ctx: SelectStatementContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitGetAllDistinct) {
			return visitor.visitGetAllDistinct(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class ProjectIndividualFieldsContext extends SelectStatementContext {
	public _field!: ProjectFieldContext;
	public selectMode(): SelectModeContext {
		return this.getRuleContext(0, SelectModeContext);
	}
	public projectField(): ProjectFieldContext[];
	public projectField(i: number): ProjectFieldContext;
	public projectField(i?: number): ProjectFieldContext | ProjectFieldContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ProjectFieldContext);
		} else {
			return this.getRuleContext(i, ProjectFieldContext);
		}
	}
	public DISTINCT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.DISTINCT, 0); }
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	public limitStatement(): LimitStatementContext | undefined {
		return this.tryGetRuleContext(0, LimitStatementContext);
	}
	constructor(ctx: SelectStatementContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitProjectIndividualFields) {
			return visitor.visitProjectIndividualFields(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class JavascriptCodeContext extends SelectStatementContext {
	public JS_SELECT(): TerminalNode { return this.getToken(BaseRqlParser.JS_SELECT, 0); }
	public JS_CL(): TerminalNode { return this.getToken(BaseRqlParser.JS_CL, 0); }
	public jsBody(): JsBodyContext[];
	public jsBody(i: number): JsBodyContext;
	public jsBody(i?: number): JsBodyContext | JsBodyContext[] {
		if (i === undefined) {
			return this.getRuleContexts(JsBodyContext);
		} else {
			return this.getRuleContext(i, JsBodyContext);
		}
	}
	public limitStatement(): LimitStatementContext | undefined {
		return this.tryGetRuleContext(0, LimitStatementContext);
	}
	constructor(ctx: SelectStatementContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitJavascriptCode) {
			return visitor.visitJavascriptCode(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ProjectFieldContext extends ParserRuleContext {
	public literal(): LiteralContext | undefined {
		return this.tryGetRuleContext(0, LiteralContext);
	}
	public specialFunctions(): SpecialFunctionsContext | undefined {
		return this.tryGetRuleContext(0, SpecialFunctionsContext);
	}
	public tsProg(): TsProgContext | undefined {
		return this.tryGetRuleContext(0, TsProgContext);
	}
	public aliasWithRequiredAs(): AliasWithRequiredAsContext | undefined {
		return this.tryGetRuleContext(0, AliasWithRequiredAsContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_projectField; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitProjectField) {
			return visitor.visitProjectField(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class JsFunctionContext extends ParserRuleContext {
	public JS_FUNCTION_DECLARATION(): TerminalNode { return this.getToken(BaseRqlParser.JS_FUNCTION_DECLARATION, 0); }
	public JFN_WORD(): TerminalNode[];
	public JFN_WORD(i: number): TerminalNode;
	public JFN_WORD(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.JFN_WORD);
		} else {
			return this.getToken(BaseRqlParser.JFN_WORD, i);
		}
	}
	public JFN_OP_PAR(): TerminalNode { return this.getToken(BaseRqlParser.JFN_OP_PAR, 0); }
	public JFN_CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.JFN_CL_PAR, 0); }
	public JFN_OP_JS(): TerminalNode { return this.getToken(BaseRqlParser.JFN_OP_JS, 0); }
	public JS_CL(): TerminalNode { return this.getToken(BaseRqlParser.JS_CL, 0); }
	public JFN_COMMA(): TerminalNode[];
	public JFN_COMMA(i: number): TerminalNode;
	public JFN_COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.JFN_COMMA);
		} else {
			return this.getToken(BaseRqlParser.JFN_COMMA, i);
		}
	}
	public jsBody(): JsBodyContext[];
	public jsBody(i: number): JsBodyContext;
	public jsBody(i?: number): JsBodyContext | JsBodyContext[] {
		if (i === undefined) {
			return this.getRuleContexts(JsBodyContext);
		} else {
			return this.getRuleContext(i, JsBodyContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_jsFunction; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitJsFunction) {
			return visitor.visitJsFunction(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class JsBodyContext extends ParserRuleContext {
	public JS_OP(): TerminalNode { return this.getToken(BaseRqlParser.JS_OP, 0); }
	public JS_CL(): TerminalNode { return this.getToken(BaseRqlParser.JS_CL, 0); }
	public jsBody(): JsBodyContext[];
	public jsBody(i: number): JsBodyContext;
	public jsBody(i?: number): JsBodyContext | JsBodyContext[] {
		if (i === undefined) {
			return this.getRuleContexts(JsBodyContext);
		} else {
			return this.getRuleContext(i, JsBodyContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_jsBody; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitJsBody) {
			return visitor.visitJsBody(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class AliasWithRequiredAsContext extends ParserRuleContext {
	public _name!: AliasNameContext;
	public AS(): TerminalNode { return this.getToken(BaseRqlParser.AS, 0); }
	public aliasName(): AliasNameContext {
		return this.getRuleContext(0, AliasNameContext);
	}
	public asArray(): AsArrayContext | undefined {
		return this.tryGetRuleContext(0, AsArrayContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_aliasWithRequiredAs; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitAliasWithRequiredAs) {
			return visitor.visitAliasWithRequiredAs(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class AliasNameContext extends ParserRuleContext {
	public WORD(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.WORD, 0); }
	public identifiersWithoutRootKeywords(): IdentifiersWithoutRootKeywordsContext | undefined {
		return this.tryGetRuleContext(0, IdentifiersWithoutRootKeywordsContext);
	}
	public string(): StringContext | undefined {
		return this.tryGetRuleContext(0, StringContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_aliasName; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitAliasName) {
			return visitor.visitAliasName(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class PrealiasContext extends ParserRuleContext {
	public METADATA(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.METADATA, 0); }
	public DOT(): TerminalNode[];
	public DOT(i: number): TerminalNode;
	public DOT(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.DOT);
		} else {
			return this.getToken(BaseRqlParser.DOT, i);
		}
	}
	public WORD(): TerminalNode[];
	public WORD(i: number): TerminalNode;
	public WORD(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.WORD);
		} else {
			return this.getToken(BaseRqlParser.WORD, i);
		}
	}
	public string(): StringContext[];
	public string(i: number): StringContext;
	public string(i?: number): StringContext | StringContext[] {
		if (i === undefined) {
			return this.getRuleContexts(StringContext);
		} else {
			return this.getRuleContext(i, StringContext);
		}
	}
	public asArray(): AsArrayContext[];
	public asArray(i: number): AsArrayContext;
	public asArray(i?: number): AsArrayContext | AsArrayContext[] {
		if (i === undefined) {
			return this.getRuleContexts(AsArrayContext);
		} else {
			return this.getRuleContext(i, AsArrayContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_prealias; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitPrealias) {
			return visitor.visitPrealias(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class AsArrayContext extends ParserRuleContext {
	public OP_Q(): TerminalNode { return this.getToken(BaseRqlParser.OP_Q, 0); }
	public CL_Q(): TerminalNode { return this.getToken(BaseRqlParser.CL_Q, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_asArray; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitAsArray) {
			return visitor.visitAsArray(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class IncludeModeContext extends ParserRuleContext {
	public INCLUDE(): TerminalNode { return this.getToken(BaseRqlParser.INCLUDE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_includeMode; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitIncludeMode) {
			return visitor.visitIncludeMode(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class IncludeStatementContext extends ParserRuleContext {
	public includeMode(): IncludeModeContext {
		return this.getRuleContext(0, IncludeModeContext);
	}
	public tsIncludeTimeseriesFunction(): TsIncludeTimeseriesFunctionContext[];
	public tsIncludeTimeseriesFunction(i: number): TsIncludeTimeseriesFunctionContext;
	public tsIncludeTimeseriesFunction(i?: number): TsIncludeTimeseriesFunctionContext | TsIncludeTimeseriesFunctionContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TsIncludeTimeseriesFunctionContext);
		} else {
			return this.getRuleContext(i, TsIncludeTimeseriesFunctionContext);
		}
	}
	public literal(): LiteralContext[];
	public literal(i: number): LiteralContext;
	public literal(i?: number): LiteralContext | LiteralContext[] {
		if (i === undefined) {
			return this.getRuleContexts(LiteralContext);
		} else {
			return this.getRuleContext(i, LiteralContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_includeStatement; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitIncludeStatement) {
			return visitor.visitIncludeStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class LimitStatementContext extends ParserRuleContext {
	public LIMIT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.LIMIT, 0); }
	public variable(): VariableContext[];
	public variable(i: number): VariableContext;
	public variable(i?: number): VariableContext | VariableContext[] {
		if (i === undefined) {
			return this.getRuleContexts(VariableContext);
		} else {
			return this.getRuleContext(i, VariableContext);
		}
	}
	public FILTER_LIMIT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.FILTER_LIMIT, 0); }
	public COMMA(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.COMMA, 0); }
	public OFFSET(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.OFFSET, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_limitStatement; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitLimitStatement) {
			return visitor.visitLimitStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class VariableContext extends ParserRuleContext {
	public _name!: MemberNameContext;
	public _member!: VariableContext;
	public DOT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.DOT, 0); }
	public memberName(): MemberNameContext {
		return this.getRuleContext(0, MemberNameContext);
	}
	public variable(): VariableContext | undefined {
		return this.tryGetRuleContext(0, VariableContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_variable; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitVariable) {
			return visitor.visitVariable(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class MemberNameContext extends ParserRuleContext {
	public cacheParam(): CacheParamContext | undefined {
		return this.tryGetRuleContext(0, CacheParamContext);
	}
	public param(): ParamContext | undefined {
		return this.tryGetRuleContext(0, ParamContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_memberName; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitMemberName) {
			return visitor.visitMemberName(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ParamContext extends ParserRuleContext {
	public NUM(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NUM, 0); }
	public WORD(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.WORD, 0); }
	public date(): DateContext | undefined {
		return this.tryGetRuleContext(0, DateContext);
	}
	public string(): StringContext | undefined {
		return this.tryGetRuleContext(0, StringContext);
	}
	public ID(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.ID, 0); }
	public OP_PAR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.OP_PAR, 0); }
	public CL_PAR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.CL_PAR, 0); }
	public identifiersAllNames(): IdentifiersAllNamesContext | undefined {
		return this.tryGetRuleContext(0, IdentifiersAllNamesContext);
	}
	public asArray(): AsArrayContext | undefined {
		return this.tryGetRuleContext(0, AsArrayContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_param; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitParam) {
			return visitor.visitParam(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class LiteralContext extends ParserRuleContext {
	public function(): FunctionContext | undefined {
		return this.tryGetRuleContext(0, FunctionContext);
	}
	public variable(): VariableContext | undefined {
		return this.tryGetRuleContext(0, VariableContext);
	}
	public DOL(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.DOL, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_literal; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitLiteral) {
			return visitor.visitLiteral(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class CacheParamContext extends ParserRuleContext {
	public DOL(): TerminalNode { return this.getToken(BaseRqlParser.DOL, 0); }
	public WORD(): TerminalNode { return this.getToken(BaseRqlParser.WORD, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_cacheParam; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitCacheParam) {
			return visitor.visitCacheParam(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ParameterWithOptionalAliasContext extends ParserRuleContext {
	public _value!: VariableOrFunctionContext;
	public _as!: AliasWithRequiredAsContext;
	public variableOrFunction(): VariableOrFunctionContext {
		return this.getRuleContext(0, VariableOrFunctionContext);
	}
	public aliasWithRequiredAs(): AliasWithRequiredAsContext | undefined {
		return this.tryGetRuleContext(0, AliasWithRequiredAsContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_parameterWithOptionalAlias; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitParameterWithOptionalAlias) {
			return visitor.visitParameterWithOptionalAlias(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class VariableOrFunctionContext extends ParserRuleContext {
	public variable(): VariableContext | undefined {
		return this.tryGetRuleContext(0, VariableContext);
	}
	public function(): FunctionContext | undefined {
		return this.tryGetRuleContext(0, FunctionContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_variableOrFunction; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitVariableOrFunction) {
			return visitor.visitVariableOrFunction(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FunctionContext extends ParserRuleContext {
	public _addr!: VariableContext;
	public _args!: ArgumentsContext;
	public OP_PAR(): TerminalNode { return this.getToken(BaseRqlParser.OP_PAR, 0); }
	public CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.CL_PAR, 0); }
	public variable(): VariableContext {
		return this.getRuleContext(0, VariableContext);
	}
	public arguments(): ArgumentsContext | undefined {
		return this.tryGetRuleContext(0, ArgumentsContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_function; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitFunction) {
			return visitor.visitFunction(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ArgumentsContext extends ParserRuleContext {
	public literal(): LiteralContext[];
	public literal(i: number): LiteralContext;
	public literal(i?: number): LiteralContext | LiteralContext[] {
		if (i === undefined) {
			return this.getRuleContexts(LiteralContext);
		} else {
			return this.getRuleContext(i, LiteralContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_arguments; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitArguments) {
			return visitor.visitArguments(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class IdentifiersWithoutRootKeywordsContext extends ParserRuleContext {
	public ALL(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.ALL, 0); }
	public AND(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.AND, 0); }
	public BETWEEN(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.BETWEEN, 0); }
	public DISTINCT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.DISTINCT, 0); }
	public ENDS_WITH(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.ENDS_WITH, 0); }
	public STARTS_WITH(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.STARTS_WITH, 0); }
	public FALSE(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.FALSE, 0); }
	public FACET(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.FACET, 0); }
	public IN(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.IN, 0); }
	public ID(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.ID, 0); }
	public INTERSECT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.INTERSECT, 0); }
	public LONG(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.LONG, 0); }
	public MATCH(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.MATCH, 0); }
	public MORELIKETHIS(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.MORELIKETHIS, 0); }
	public NULL(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NULL, 0); }
	public OR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.OR, 0); }
	public STRING_W(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.STRING_W, 0); }
	public TRUE(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TRUE, 0); }
	public WITH(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.WITH, 0); }
	public EXACT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.EXACT, 0); }
	public BOOST(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.BOOST, 0); }
	public SEARCH(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.SEARCH, 0); }
	public FUZZY(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.FUZZY, 0); }
	public METADATA(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.METADATA, 0); }
	public TO(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TO, 0); }
	public NOT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NOT, 0); }
	public SORTING(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.SORTING, 0); }
	public ALPHANUMERIC(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.ALPHANUMERIC, 0); }
	public DOUBLE(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.DOUBLE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_identifiersWithoutRootKeywords; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitIdentifiersWithoutRootKeywords) {
			return visitor.visitIdentifiersWithoutRootKeywords(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class RootKeywordsContext extends ParserRuleContext {
	public FROM(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.FROM, 0); }
	public GROUP_BY(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.GROUP_BY, 0); }
	public WHERE(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.WHERE, 0); }
	public LOAD(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.LOAD, 0); }
	public ORDER_BY(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.ORDER_BY, 0); }
	public SELECT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.SELECT, 0); }
	public INCLUDE(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.INCLUDE, 0); }
	public LIMIT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.LIMIT, 0); }
	public INDEX(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.INDEX, 0); }
	public FILTER(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.FILTER, 0); }
	public FILTER_LIMIT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.FILTER_LIMIT, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_rootKeywords; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitRootKeywords) {
			return visitor.visitRootKeywords(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class IdentifiersAllNamesContext extends ParserRuleContext {
	public identifiersWithoutRootKeywords(): IdentifiersWithoutRootKeywordsContext | undefined {
		return this.tryGetRuleContext(0, IdentifiersWithoutRootKeywordsContext);
	}
	public rootKeywords(): RootKeywordsContext | undefined {
		return this.tryGetRuleContext(0, RootKeywordsContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_identifiersAllNames; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitIdentifiersAllNames) {
			return visitor.visitIdentifiersAllNames(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class DateContext extends ParserRuleContext {
	public OP_Q(): TerminalNode { return this.getToken(BaseRqlParser.OP_Q, 0); }
	public TO(): TerminalNode { return this.getToken(BaseRqlParser.TO, 0); }
	public CL_Q(): TerminalNode { return this.getToken(BaseRqlParser.CL_Q, 0); }
	public NULL(): TerminalNode[];
	public NULL(i: number): TerminalNode;
	public NULL(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.NULL);
		} else {
			return this.getToken(BaseRqlParser.NULL, i);
		}
	}
	public dateString(): DateStringContext[];
	public dateString(i: number): DateStringContext;
	public dateString(i?: number): DateStringContext | DateStringContext[] {
		if (i === undefined) {
			return this.getRuleContexts(DateStringContext);
		} else {
			return this.getRuleContext(i, DateStringContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_date; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitDate) {
			return visitor.visitDate(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class DateStringContext extends ParserRuleContext {
	public WORD(): TerminalNode { return this.getToken(BaseRqlParser.WORD, 0); }
	public DOT(): TerminalNode { return this.getToken(BaseRqlParser.DOT, 0); }
	public NUM(): TerminalNode { return this.getToken(BaseRqlParser.NUM, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_dateString; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitDateString) {
			return visitor.visitDateString(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsProgContext extends ParserRuleContext {
	public TIMESERIES(): TerminalNode { return this.getToken(BaseRqlParser.TIMESERIES, 0); }
	public tsQueryBody(): TsQueryBodyContext {
		return this.getRuleContext(0, TsQueryBodyContext);
	}
	public TS_CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.TS_CL_PAR, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsProg; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsProg) {
			return visitor.visitTsProg(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsIncludeTimeseriesFunctionContext extends ParserRuleContext {
	public TIMESERIES(): TerminalNode { return this.getToken(BaseRqlParser.TIMESERIES, 0); }
	public tsLiteral(): TsLiteralContext {
		return this.getRuleContext(0, TsLiteralContext);
	}
	public TS_CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.TS_CL_PAR, 0); }
	public TS_COMMA(): TerminalNode[];
	public TS_COMMA(i: number): TerminalNode;
	public TS_COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.TS_COMMA);
		} else {
			return this.getToken(BaseRqlParser.TS_COMMA, i);
		}
	}
	public tsIncludeLiteral(): TsIncludeLiteralContext[];
	public tsIncludeLiteral(i: number): TsIncludeLiteralContext;
	public tsIncludeLiteral(i?: number): TsIncludeLiteralContext | TsIncludeLiteralContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TsIncludeLiteralContext);
		} else {
			return this.getRuleContext(i, TsIncludeLiteralContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsIncludeTimeseriesFunction; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsIncludeTimeseriesFunction) {
			return visitor.visitTsIncludeTimeseriesFunction(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsIncludeLiteralContext extends ParserRuleContext {
	public tsLiteral(): TsLiteralContext | undefined {
		return this.tryGetRuleContext(0, TsLiteralContext);
	}
	public tsIncludeSpecialMethod(): TsIncludeSpecialMethodContext | undefined {
		return this.tryGetRuleContext(0, TsIncludeSpecialMethodContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsIncludeLiteral; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsIncludeLiteral) {
			return visitor.visitTsIncludeLiteral(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsIncludeSpecialMethodContext extends ParserRuleContext {
	public TS_OP_PAR(): TerminalNode { return this.getToken(BaseRqlParser.TS_OP_PAR, 0); }
	public TS_NUM(): TerminalNode { return this.getToken(BaseRqlParser.TS_NUM, 0); }
	public TS_CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.TS_CL_PAR, 0); }
	public TS_LAST(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_LAST, 0); }
	public TS_FIRST(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_FIRST, 0); }
	public TS_COMMA(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_COMMA, 0); }
	public TS_STRING(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_STRING, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsIncludeSpecialMethod; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsIncludeSpecialMethod) {
			return visitor.visitTsIncludeSpecialMethod(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsQueryBodyContext extends ParserRuleContext {
	public _from!: TsFROMContext;
	public _range!: TsTimeRangeStatementContext;
	public _load!: TsLoadStatementContext;
	public _where!: TsWHEREContext;
	public _groupBy!: TsGroupByContext;
	public _select!: TsSelectContext;
	public _scale!: TsSelectScaleProjectionContext;
	public _offset!: TsOffsetContext;
	public tsFROM(): TsFROMContext {
		return this.getRuleContext(0, TsFROMContext);
	}
	public tsTimeRangeStatement(): TsTimeRangeStatementContext | undefined {
		return this.tryGetRuleContext(0, TsTimeRangeStatementContext);
	}
	public tsLoadStatement(): TsLoadStatementContext | undefined {
		return this.tryGetRuleContext(0, TsLoadStatementContext);
	}
	public tsWHERE(): TsWHEREContext | undefined {
		return this.tryGetRuleContext(0, TsWHEREContext);
	}
	public tsGroupBy(): TsGroupByContext | undefined {
		return this.tryGetRuleContext(0, TsGroupByContext);
	}
	public tsSelect(): TsSelectContext | undefined {
		return this.tryGetRuleContext(0, TsSelectContext);
	}
	public tsSelectScaleProjection(): TsSelectScaleProjectionContext | undefined {
		return this.tryGetRuleContext(0, TsSelectScaleProjectionContext);
	}
	public tsOffset(): TsOffsetContext | undefined {
		return this.tryGetRuleContext(0, TsOffsetContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsQueryBody; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsQueryBody) {
			return visitor.visitTsQueryBody(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsOffsetContext extends ParserRuleContext {
	public TS_OFFSET(): TerminalNode { return this.getToken(BaseRqlParser.TS_OFFSET, 0); }
	public TS_STRING(): TerminalNode { return this.getToken(BaseRqlParser.TS_STRING, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsOffset; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsOffset) {
			return visitor.visitTsOffset(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsFunctionContext extends ParserRuleContext {
	public _body!: TsQueryBodyContext;
	public TIMESERIES_FUNCTION_DECLARATION(): TerminalNode { return this.getToken(BaseRqlParser.TIMESERIES_FUNCTION_DECLARATION, 0); }
	public TS_WORD(): TerminalNode[];
	public TS_WORD(i: number): TerminalNode;
	public TS_WORD(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.TS_WORD);
		} else {
			return this.getToken(BaseRqlParser.TS_WORD, i);
		}
	}
	public TS_OP_PAR(): TerminalNode { return this.getToken(BaseRqlParser.TS_OP_PAR, 0); }
	public TS_CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.TS_CL_PAR, 0); }
	public TS_OP_C(): TerminalNode { return this.getToken(BaseRqlParser.TS_OP_C, 0); }
	public TS_CL_C(): TerminalNode { return this.getToken(BaseRqlParser.TS_CL_C, 0); }
	public tsQueryBody(): TsQueryBodyContext {
		return this.getRuleContext(0, TsQueryBodyContext);
	}
	public TS_COMMA(): TerminalNode[];
	public TS_COMMA(i: number): TerminalNode;
	public TS_COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.TS_COMMA);
		} else {
			return this.getToken(BaseRqlParser.TS_COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsFunction; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsFunction) {
			return visitor.visitTsFunction(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsTimeRangeStatementContext extends ParserRuleContext {
	public _first!: TsTimeRangeFirstContext;
	public _last!: TsTimeRangeLastContext;
	public tsBetween(): TsBetweenContext | undefined {
		return this.tryGetRuleContext(0, TsBetweenContext);
	}
	public tsTimeRangeFirst(): TsTimeRangeFirstContext | undefined {
		return this.tryGetRuleContext(0, TsTimeRangeFirstContext);
	}
	public tsTimeRangeLast(): TsTimeRangeLastContext | undefined {
		return this.tryGetRuleContext(0, TsTimeRangeLastContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsTimeRangeStatement; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsTimeRangeStatement) {
			return visitor.visitTsTimeRangeStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsLoadStatementContext extends ParserRuleContext {
	public TS_LOAD(): TerminalNode { return this.getToken(BaseRqlParser.TS_LOAD, 0); }
	public tsAlias(): TsAliasContext {
		return this.getRuleContext(0, TsAliasContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsLoadStatement; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsLoadStatement) {
			return visitor.visitTsLoadStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsAliasContext extends ParserRuleContext {
	public _alias_text!: TsLiteralContext;
	public TS_AS(): TerminalNode { return this.getToken(BaseRqlParser.TS_AS, 0); }
	public tsLiteral(): TsLiteralContext {
		return this.getRuleContext(0, TsLiteralContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsAlias; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsAlias) {
			return visitor.visitTsAlias(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsFROMContext extends ParserRuleContext {
	public _name!: TsCollectionNameContext;
	public TS_FROM(): TerminalNode { return this.getToken(BaseRqlParser.TS_FROM, 0); }
	public tsCollectionName(): TsCollectionNameContext {
		return this.getRuleContext(0, TsCollectionNameContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsFROM; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsFROM) {
			return visitor.visitTsFROM(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsWHEREContext extends ParserRuleContext {
	public TS_WHERE(): TerminalNode { return this.getToken(BaseRqlParser.TS_WHERE, 0); }
	public tsExpr(): TsExprContext {
		return this.getRuleContext(0, TsExprContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsWHERE; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsWHERE) {
			return visitor.visitTsWHERE(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsExprContext extends ParserRuleContext {
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsExpr; }
	public copyFrom(ctx: TsExprContext): void {
		super.copyFrom(ctx);
	}
}
export class TsMathExpressionContext extends TsExprContext {
	public _left!: TsExprContext;
	public _right!: TsExprContext;
	public TS_MATH(): TerminalNode { return this.getToken(BaseRqlParser.TS_MATH, 0); }
	public tsExpr(): TsExprContext[];
	public tsExpr(i: number): TsExprContext;
	public tsExpr(i?: number): TsExprContext | TsExprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TsExprContext);
		} else {
			return this.getRuleContext(i, TsExprContext);
		}
	}
	constructor(ctx: TsExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsMathExpression) {
			return visitor.visitTsMathExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class TsBinaryExpressionContext extends TsExprContext {
	public _left!: TsExprContext;
	public _right!: TsExprContext;
	public tsBinary(): TsBinaryContext {
		return this.getRuleContext(0, TsBinaryContext);
	}
	public tsExpr(): TsExprContext[];
	public tsExpr(i: number): TsExprContext;
	public tsExpr(i?: number): TsExprContext | TsExprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TsExprContext);
		} else {
			return this.getRuleContext(i, TsExprContext);
		}
	}
	constructor(ctx: TsExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsBinaryExpression) {
			return visitor.visitTsBinaryExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class TsOpParContext extends TsExprContext {
	public TS_OP_PAR(): TerminalNode { return this.getToken(BaseRqlParser.TS_OP_PAR, 0); }
	public tsExpr(): TsExprContext {
		return this.getRuleContext(0, TsExprContext);
	}
	public TS_CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.TS_CL_PAR, 0); }
	constructor(ctx: TsExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsOpPar) {
			return visitor.visitTsOpPar(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class TsBooleanExpressionContext extends TsExprContext {
	public TS_TRUE(): TerminalNode { return this.getToken(BaseRqlParser.TS_TRUE, 0); }
	public TS_AND(): TerminalNode { return this.getToken(BaseRqlParser.TS_AND, 0); }
	public tsExpr(): TsExprContext {
		return this.getRuleContext(0, TsExprContext);
	}
	public TS_NOT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_NOT, 0); }
	constructor(ctx: TsExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsBooleanExpression) {
			return visitor.visitTsBooleanExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class TsLiteralExpressionContext extends TsExprContext {
	public tsLiteral(): TsLiteralContext {
		return this.getRuleContext(0, TsLiteralContext);
	}
	constructor(ctx: TsExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsLiteralExpression) {
			return visitor.visitTsLiteralExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsBetweenContext extends ParserRuleContext {
	public _from!: TsLiteralContext;
	public _to!: TsLiteralContext;
	public TS_BETWEEN(): TerminalNode { return this.getToken(BaseRqlParser.TS_BETWEEN, 0); }
	public TS_AND(): TerminalNode { return this.getToken(BaseRqlParser.TS_AND, 0); }
	public tsLiteral(): TsLiteralContext[];
	public tsLiteral(i: number): TsLiteralContext;
	public tsLiteral(i?: number): TsLiteralContext | TsLiteralContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TsLiteralContext);
		} else {
			return this.getRuleContext(i, TsLiteralContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsBetween; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsBetween) {
			return visitor.visitTsBetween(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsBinaryContext extends ParserRuleContext {
	public TS_AND(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_AND, 0); }
	public TS_OR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_OR, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsBinary; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsBinary) {
			return visitor.visitTsBinary(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsLiteralContext extends ParserRuleContext {
	public TS_DOL(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_DOL, 0); }
	public TS_WORD(): TerminalNode[];
	public TS_WORD(i: number): TerminalNode;
	public TS_WORD(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.TS_WORD);
		} else {
			return this.getToken(BaseRqlParser.TS_WORD, i);
		}
	}
	public TS_NUM(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_NUM, 0); }
	public tsIdentifiers(): TsIdentifiersContext[];
	public tsIdentifiers(i: number): TsIdentifiersContext;
	public tsIdentifiers(i?: number): TsIdentifiersContext | TsIdentifiersContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TsIdentifiersContext);
		} else {
			return this.getRuleContext(i, TsIdentifiersContext);
		}
	}
	public TS_STRING(): TerminalNode[];
	public TS_STRING(i: number): TerminalNode;
	public TS_STRING(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.TS_STRING);
		} else {
			return this.getToken(BaseRqlParser.TS_STRING, i);
		}
	}
	public TS_OP_Q(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_OP_Q, 0); }
	public TS_CL_Q(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_CL_Q, 0); }
	public TS_DOT(): TerminalNode[];
	public TS_DOT(i: number): TerminalNode;
	public TS_DOT(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.TS_DOT);
		} else {
			return this.getToken(BaseRqlParser.TS_DOT, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsLiteral; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsLiteral) {
			return visitor.visitTsLiteral(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsTimeRangeFirstContext extends ParserRuleContext {
	public _num!: Token;
	public _size!: Token;
	public TS_FIRST(): TerminalNode { return this.getToken(BaseRqlParser.TS_FIRST, 0); }
	public TS_NUM(): TerminalNode { return this.getToken(BaseRqlParser.TS_NUM, 0); }
	public TS_TIMERANGE(): TerminalNode { return this.getToken(BaseRqlParser.TS_TIMERANGE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsTimeRangeFirst; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsTimeRangeFirst) {
			return visitor.visitTsTimeRangeFirst(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsTimeRangeLastContext extends ParserRuleContext {
	public _num!: Token;
	public _size!: Token;
	public TS_LAST(): TerminalNode { return this.getToken(BaseRqlParser.TS_LAST, 0); }
	public TS_NUM(): TerminalNode { return this.getToken(BaseRqlParser.TS_NUM, 0); }
	public TS_TIMERANGE(): TerminalNode { return this.getToken(BaseRqlParser.TS_TIMERANGE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsTimeRangeLast; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsTimeRangeLast) {
			return visitor.visitTsTimeRangeLast(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsCollectionNameContext extends ParserRuleContext {
	public TS_WORD(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_WORD, 0); }
	public TS_STRING(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_STRING, 0); }
	public tsIdentifiers(): TsIdentifiersContext | undefined {
		return this.tryGetRuleContext(0, TsIdentifiersContext);
	}
	public TS_DOT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_DOT, 0); }
	public tsCollectionName(): TsCollectionNameContext | undefined {
		return this.tryGetRuleContext(0, TsCollectionNameContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsCollectionName; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsCollectionName) {
			return visitor.visitTsCollectionName(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsGroupByContext extends ParserRuleContext {
	public _name!: TsCollectionNameContext;
	public TS_GROUPBY(): TerminalNode { return this.getToken(BaseRqlParser.TS_GROUPBY, 0); }
	public tsCollectionName(): TsCollectionNameContext[];
	public tsCollectionName(i: number): TsCollectionNameContext;
	public tsCollectionName(i?: number): TsCollectionNameContext | TsCollectionNameContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TsCollectionNameContext);
		} else {
			return this.getRuleContext(i, TsCollectionNameContext);
		}
	}
	public TS_COMMA(): TerminalNode[];
	public TS_COMMA(i: number): TerminalNode;
	public TS_COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.TS_COMMA);
		} else {
			return this.getToken(BaseRqlParser.TS_COMMA, i);
		}
	}
	public TS_WITH(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_WITH, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsGroupBy; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsGroupBy) {
			return visitor.visitTsGroupBy(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsSelectContext extends ParserRuleContext {
	public _field!: TsSelectVariableContext;
	public TS_SELECT(): TerminalNode { return this.getToken(BaseRqlParser.TS_SELECT, 0); }
	public tsSelectVariable(): TsSelectVariableContext[];
	public tsSelectVariable(i: number): TsSelectVariableContext;
	public tsSelectVariable(i?: number): TsSelectVariableContext | TsSelectVariableContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TsSelectVariableContext);
		} else {
			return this.getRuleContext(i, TsSelectVariableContext);
		}
	}
	public TS_COMMA(): TerminalNode[];
	public TS_COMMA(i: number): TerminalNode;
	public TS_COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.TS_COMMA);
		} else {
			return this.getToken(BaseRqlParser.TS_COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsSelect; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsSelect) {
			return visitor.visitTsSelect(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsSelectScaleProjectionContext extends ParserRuleContext {
	public TS_SCALE(): TerminalNode { return this.getToken(BaseRqlParser.TS_SCALE, 0); }
	public TS_NUM(): TerminalNode { return this.getToken(BaseRqlParser.TS_NUM, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsSelectScaleProjection; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsSelectScaleProjection) {
			return visitor.visitTsSelectScaleProjection(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsSelectVariableContext extends ParserRuleContext {
	public TS_METHOD(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_METHOD, 0); }
	public tsLiteral(): TsLiteralContext | undefined {
		return this.tryGetRuleContext(0, TsLiteralContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsSelectVariable; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsSelectVariable) {
			return visitor.visitTsSelectVariable(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TsIdentifiersContext extends ParserRuleContext {
	public TS_OR(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_OR, 0); }
	public TS_AND(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_AND, 0); }
	public TS_FROM(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_FROM, 0); }
	public TS_WHERE(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_WHERE, 0); }
	public TS_GROUPBY(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_GROUPBY, 0); }
	public TS_TIMERANGE(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TS_TIMERANGE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_tsIdentifiers; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitTsIdentifiers) {
			return visitor.visitTsIdentifiers(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class UpdateBodyContext extends ParserRuleContext {
	public US_OP(): TerminalNode { return this.getToken(BaseRqlParser.US_OP, 0); }
	public US_CL(): TerminalNode { return this.getToken(BaseRqlParser.US_CL, 0); }
	public updateBody(): UpdateBodyContext[];
	public updateBody(i: number): UpdateBodyContext;
	public updateBody(i?: number): UpdateBodyContext | UpdateBodyContext[] {
		if (i === undefined) {
			return this.getRuleContexts(UpdateBodyContext);
		} else {
			return this.getRuleContext(i, UpdateBodyContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_updateBody; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitUpdateBody) {
			return visitor.visitUpdateBody(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FilterStatementContext extends ParserRuleContext {
	public filterMode(): FilterModeContext {
		return this.getRuleContext(0, FilterModeContext);
	}
	public filterExpr(): FilterExprContext {
		return this.getRuleContext(0, FilterExprContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_filterStatement; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitFilterStatement) {
			return visitor.visitFilterStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FilterExprContext extends ParserRuleContext {
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_filterExpr; }
	public copyFrom(ctx: FilterExprContext): void {
		super.copyFrom(ctx);
	}
}
export class FilterBinaryExpressionContext extends FilterExprContext {
	public _left!: FilterExprContext;
	public _right!: FilterExprContext;
	public binary(): BinaryContext {
		return this.getRuleContext(0, BinaryContext);
	}
	public filterExpr(): FilterExprContext[];
	public filterExpr(i: number): FilterExprContext;
	public filterExpr(i?: number): FilterExprContext | FilterExprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(FilterExprContext);
		} else {
			return this.getRuleContext(i, FilterExprContext);
		}
	}
	constructor(ctx: FilterExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitFilterBinaryExpression) {
			return visitor.visitFilterBinaryExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class FilterOpParContext extends FilterExprContext {
	public OP_PAR(): TerminalNode { return this.getToken(BaseRqlParser.OP_PAR, 0); }
	public filterExpr(): FilterExprContext {
		return this.getRuleContext(0, FilterExprContext);
	}
	public CL_PAR(): TerminalNode { return this.getToken(BaseRqlParser.CL_PAR, 0); }
	constructor(ctx: FilterExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitFilterOpPar) {
			return visitor.visitFilterOpPar(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class FilterEqualExpressionContext extends FilterExprContext {
	public _left!: ExprValueContext;
	public _right!: ExprValueContext;
	public EQUAL(): TerminalNode { return this.getToken(BaseRqlParser.EQUAL, 0); }
	public exprValue(): ExprValueContext[];
	public exprValue(i: number): ExprValueContext;
	public exprValue(i?: number): ExprValueContext | ExprValueContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ExprValueContext);
		} else {
			return this.getRuleContext(i, ExprValueContext);
		}
	}
	constructor(ctx: FilterExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitFilterEqualExpression) {
			return visitor.visitFilterEqualExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class FilterMathExpressionContext extends FilterExprContext {
	public _left!: ExprValueContext;
	public _right!: ExprValueContext;
	public MATH(): TerminalNode { return this.getToken(BaseRqlParser.MATH, 0); }
	public exprValue(): ExprValueContext[];
	public exprValue(i: number): ExprValueContext;
	public exprValue(i?: number): ExprValueContext | ExprValueContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ExprValueContext);
		} else {
			return this.getRuleContext(i, ExprValueContext);
		}
	}
	constructor(ctx: FilterExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitFilterMathExpression) {
			return visitor.visitFilterMathExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class FilterNormalFuncContext extends FilterExprContext {
	public _funcExpr!: FunctionContext;
	public function(): FunctionContext {
		return this.getRuleContext(0, FunctionContext);
	}
	constructor(ctx: FilterExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitFilterNormalFunc) {
			return visitor.visitFilterNormalFunc(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
export class FilterBooleanExpressionContext extends FilterExprContext {
	public TRUE(): TerminalNode { return this.getToken(BaseRqlParser.TRUE, 0); }
	public AND(): TerminalNode { return this.getToken(BaseRqlParser.AND, 0); }
	public expr(): ExprContext {
		return this.getRuleContext(0, ExprContext);
	}
	public NOT(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NOT, 0); }
	constructor(ctx: FilterExprContext) {
		super(ctx.parent, ctx.invokingState);
		this.copyFrom(ctx);
	}
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitFilterBooleanExpression) {
			return visitor.visitFilterBooleanExpression(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FilterModeContext extends ParserRuleContext {
	public FILTER(): TerminalNode { return this.getToken(BaseRqlParser.FILTER, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_filterMode; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitFilterMode) {
			return visitor.visitFilterMode(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ParameterBeforeQueryContext extends ParserRuleContext {
	public _name!: LiteralContext;
	public _value!: ParameterValueContext;
	public EQUAL(): TerminalNode { return this.getToken(BaseRqlParser.EQUAL, 0); }
	public literal(): LiteralContext {
		return this.getRuleContext(0, LiteralContext);
	}
	public parameterValue(): ParameterValueContext {
		return this.getRuleContext(0, ParameterValueContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_parameterBeforeQuery; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitParameterBeforeQuery) {
			return visitor.visitParameterBeforeQuery(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ParameterValueContext extends ParserRuleContext {
	public string(): StringContext | undefined {
		return this.tryGetRuleContext(0, StringContext);
	}
	public NUM(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NUM, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_parameterValue; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitParameterValue) {
			return visitor.visitParameterValue(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class JsonContext extends ParserRuleContext {
	public jsonObj(): JsonObjContext {
		return this.getRuleContext(0, JsonObjContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_json; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitJson) {
			return visitor.visitJson(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class JsonObjContext extends ParserRuleContext {
	public OP_CUR(): TerminalNode { return this.getToken(BaseRqlParser.OP_CUR, 0); }
	public jsonPair(): JsonPairContext[];
	public jsonPair(i: number): JsonPairContext;
	public jsonPair(i?: number): JsonPairContext | JsonPairContext[] {
		if (i === undefined) {
			return this.getRuleContexts(JsonPairContext);
		} else {
			return this.getRuleContext(i, JsonPairContext);
		}
	}
	public CL_CUR(): TerminalNode { return this.getToken(BaseRqlParser.CL_CUR, 0); }
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_jsonObj; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitJsonObj) {
			return visitor.visitJsonObj(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class JsonPairContext extends ParserRuleContext {
	public DOUBLE_QUOTE_STRING(): TerminalNode { return this.getToken(BaseRqlParser.DOUBLE_QUOTE_STRING, 0); }
	public COLON(): TerminalNode { return this.getToken(BaseRqlParser.COLON, 0); }
	public jsonValue(): JsonValueContext {
		return this.getRuleContext(0, JsonValueContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_jsonPair; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitJsonPair) {
			return visitor.visitJsonPair(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class JsonArrContext extends ParserRuleContext {
	public OP_Q(): TerminalNode { return this.getToken(BaseRqlParser.OP_Q, 0); }
	public jsonValue(): JsonValueContext[];
	public jsonValue(i: number): JsonValueContext;
	public jsonValue(i?: number): JsonValueContext | JsonValueContext[] {
		if (i === undefined) {
			return this.getRuleContexts(JsonValueContext);
		} else {
			return this.getRuleContext(i, JsonValueContext);
		}
	}
	public CL_Q(): TerminalNode { return this.getToken(BaseRqlParser.CL_Q, 0); }
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(BaseRqlParser.COMMA);
		} else {
			return this.getToken(BaseRqlParser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_jsonArr; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitJsonArr) {
			return visitor.visitJsonArr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class JsonValueContext extends ParserRuleContext {
	public DOUBLE_QUOTE_STRING(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.DOUBLE_QUOTE_STRING, 0); }
	public NUM(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NUM, 0); }
	public jsonObj(): JsonObjContext | undefined {
		return this.tryGetRuleContext(0, JsonObjContext);
	}
	public jsonArr(): JsonArrContext | undefined {
		return this.tryGetRuleContext(0, JsonArrContext);
	}
	public TRUE(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.TRUE, 0); }
	public FALSE(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.FALSE, 0); }
	public NULL(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.NULL, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_jsonValue; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitJsonValue) {
			return visitor.visitJsonValue(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class StringContext extends ParserRuleContext {
	public DOUBLE_QUOTE_STRING(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.DOUBLE_QUOTE_STRING, 0); }
	public SINGLE_QUOTE_STRING(): TerminalNode | undefined { return this.tryGetToken(BaseRqlParser.SINGLE_QUOTE_STRING, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return BaseRqlParser.RULE_string; }
	// @Override
	public accept<Result>(visitor: BaseRqlParserVisitor<Result>): Result {
		if (visitor.visitString) {
			return visitor.visitString(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


