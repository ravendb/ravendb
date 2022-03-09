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
	public static readonly OFFSET = 55;
	public static readonly SELECT = 56;
	public static readonly JS_SELECT = 57;
	public static readonly SORTING = 58;
	public static readonly STRING_W = 59;
	public static readonly TO = 60;
	public static readonly TRUE = 61;
	public static readonly WHERE = 62;
	public static readonly WITH = 63;
	public static readonly EXACT = 64;
	public static readonly BOOST = 65;
	public static readonly SEARCH = 66;
	public static readonly LIMIT = 67;
	public static readonly FUZZY = 68;
	public static readonly FILTER = 69;
	public static readonly FILTER_LIMIT = 70;
	public static readonly TIMESERIES = 71;
	public static readonly JS_FUNCTION_DECLARATION = 72;
	public static readonly TIMESERIES_FUNCTION_DECLARATION = 73;
	public static readonly NUM = 74;
	public static readonly DOUBLE_QUOTE_STRING = 75;
	public static readonly SINGLE_QUOTE_STRING = 76;
	public static readonly WORD = 77;
	public static readonly WS = 78;
	public static readonly TS_METHOD = 79;
	public static readonly TS_OP_C = 80;
	public static readonly TS_CL_C = 81;
	public static readonly TS_OP_PAR = 82;
	public static readonly TS_CL_PAR = 83;
	public static readonly TS_OP_Q = 84;
	public static readonly TS_CL_Q = 85;
	public static readonly TS_DOT = 86;
	public static readonly TS_COMMA = 87;
	public static readonly TS_DOL = 88;
	public static readonly TS_MATH = 89;
	public static readonly TS_OR = 90;
	public static readonly TS_TRUE = 91;
	public static readonly TS_NOT = 92;
	public static readonly TS_AS = 93;
	public static readonly TS_AND = 94;
	public static readonly TS_FROM = 95;
	public static readonly TS_WHERE = 96;
	public static readonly TS_GROUPBY = 97;
	public static readonly TS_BETWEEN = 98;
	public static readonly TS_FIRST = 99;
	public static readonly TS_LAST = 100;
	public static readonly TS_WITH = 101;
	public static readonly TS_TIMERANGE = 102;
	public static readonly TS_GROUPBY_VALUE = 103;
	public static readonly TS_SELECT = 104;
	public static readonly TS_LOAD = 105;
	public static readonly TS_SCALE = 106;
	public static readonly TS_OFFSET = 107;
	public static readonly TS_NUM = 108;
	public static readonly TS_STRING = 109;
	public static readonly TS_SINGLE_QUOTE_STRING = 110;
	public static readonly TS_WORD = 111;
	public static readonly TS_WS = 112;
	public static readonly US_OP = 113;
	public static readonly US_CL = 114;
	public static readonly US_WS = 115;
	public static readonly US_DATA = 116;
	public static readonly JS_OP = 117;
	public static readonly JS_CL = 118;
	public static readonly JS_DATA = 119;
	public static readonly JFN_WORD = 120;
	public static readonly JFN_OP_PAR = 121;
	public static readonly JFN_CL_PAR = 122;
	public static readonly JFN_OP_JS = 123;
	public static readonly JFN_COMMA = 124;
	public static readonly JFN_WS = 125;
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
	public static readonly RULE_specialParam = 20;
	public static readonly RULE_loadMode = 21;
	public static readonly RULE_loadStatement = 22;
	public static readonly RULE_loadDocumentByName = 23;
	public static readonly RULE_orderByMode = 24;
	public static readonly RULE_orderByStatement = 25;
	public static readonly RULE_orderByItem = 26;
	public static readonly RULE_orderBySorting = 27;
	public static readonly RULE_orderBySortingAs = 28;
	public static readonly RULE_orderByOrder = 29;
	public static readonly RULE_selectMode = 30;
	public static readonly RULE_selectStatement = 31;
	public static readonly RULE_projectField = 32;
	public static readonly RULE_jsFunction = 33;
	public static readonly RULE_jsBody = 34;
	public static readonly RULE_aliasWithRequiredAs = 35;
	public static readonly RULE_aliasName = 36;
	public static readonly RULE_prealias = 37;
	public static readonly RULE_asArray = 38;
	public static readonly RULE_includeMode = 39;
	public static readonly RULE_includeStatement = 40;
	public static readonly RULE_limitStatement = 41;
	public static readonly RULE_variable = 42;
	public static readonly RULE_memberName = 43;
	public static readonly RULE_param = 44;
	public static readonly RULE_literal = 45;
	public static readonly RULE_cacheParam = 46;
	public static readonly RULE_parameterWithOptionalAlias = 47;
	public static readonly RULE_variableOrFunction = 48;
	public static readonly RULE_function = 49;
	public static readonly RULE_arguments = 50;
	public static readonly RULE_identifiersWithoutRootKeywords = 51;
	public static readonly RULE_rootKeywords = 52;
	public static readonly RULE_identifiersAllNames = 53;
	public static readonly RULE_date = 54;
	public static readonly RULE_dateString = 55;
	public static readonly RULE_tsProg = 56;
	public static readonly RULE_tsIncludeTimeseriesFunction = 57;
	public static readonly RULE_tsIncludeLiteral = 58;
	public static readonly RULE_tsIncludeSpecialMethod = 59;
	public static readonly RULE_tsQueryBody = 60;
	public static readonly RULE_tsOffset = 61;
	public static readonly RULE_tsFunction = 62;
	public static readonly RULE_tsTimeRangeStatement = 63;
	public static readonly RULE_tsLoadStatement = 64;
	public static readonly RULE_tsAlias = 65;
	public static readonly RULE_tsFROM = 66;
	public static readonly RULE_tsWHERE = 67;
	public static readonly RULE_tsExpr = 68;
	public static readonly RULE_tsBetween = 69;
	public static readonly RULE_tsBinary = 70;
	public static readonly RULE_tsLiteral = 71;
	public static readonly RULE_tsTimeRangeFirst = 72;
	public static readonly RULE_tsTimeRangeLast = 73;
	public static readonly RULE_tsCollectionName = 74;
	public static readonly RULE_tsGroupBy = 75;
	public static readonly RULE_tsSelect = 76;
	public static readonly RULE_tsSelectScaleProjection = 77;
	public static readonly RULE_tsSelectVariable = 78;
	public static readonly RULE_tsIdentifiers = 79;
	public static readonly RULE_updateBody = 80;
	public static readonly RULE_filterStatement = 81;
	public static readonly RULE_filterExpr = 82;
	public static readonly RULE_filterMode = 83;
	public static readonly RULE_parameterBeforeQuery = 84;
	public static readonly RULE_parameterValue = 85;
	public static readonly RULE_json = 86;
	public static readonly RULE_jsonObj = 87;
	public static readonly RULE_jsonPair = 88;
	public static readonly RULE_jsonArr = 89;
	public static readonly RULE_jsonValue = 90;
	public static readonly RULE_string = 91;
	// tslint:disable:no-trailing-whitespace
	public static readonly ruleNames: string[] = [
		"prog", "functionStatment", "updateStatement", "fromMode", "fromStatement", 
		"indexName", "collectionName", "aliasWithOptionalAs", "groupByMode", "groupByStatement", 
		"suggestGroupBy", "whereMode", "whereStatement", "expr", "binary", "exprValue", 
		"inFunction", "betweenFunction", "specialFunctions", "specialFunctionName", 
		"specialParam", "loadMode", "loadStatement", "loadDocumentByName", "orderByMode", 
		"orderByStatement", "orderByItem", "orderBySorting", "orderBySortingAs", 
		"orderByOrder", "selectMode", "selectStatement", "projectField", "jsFunction", 
		"jsBody", "aliasWithRequiredAs", "aliasName", "prealias", "asArray", "includeMode", 
		"includeStatement", "limitStatement", "variable", "memberName", "param", 
		"literal", "cacheParam", "parameterWithOptionalAlias", "variableOrFunction", 
		"function", "arguments", "identifiersWithoutRootKeywords", "rootKeywords", 
		"identifiersAllNames", "date", "dateString", "tsProg", "tsIncludeTimeseriesFunction", 
		"tsIncludeLiteral", "tsIncludeSpecialMethod", "tsQueryBody", "tsOffset", 
		"tsFunction", "tsTimeRangeStatement", "tsLoadStatement", "tsAlias", "tsFROM", 
		"tsWHERE", "tsExpr", "tsBetween", "tsBinary", "tsLiteral", "tsTimeRangeFirst", 
		"tsTimeRangeLast", "tsCollectionName", "tsGroupBy", "tsSelect", "tsSelectScaleProjection", 
		"tsSelectVariable", "tsIdentifiers", "updateBody", "filterStatement", 
		"filterExpr", "filterMode", "parameterBeforeQuery", "parameterValue", 
		"json", "jsonObj", "jsonPair", "jsonArr", "jsonValue", "string",
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
		"NULL", "OR", "ORDER_BY", "OFFSET", "SELECT", "JS_SELECT", "SORTING", 
		"STRING_W", "TO", "TRUE", "WHERE", "WITH", "EXACT", "BOOST", "SEARCH", 
		"LIMIT", "FUZZY", "FILTER", "FILTER_LIMIT", "TIMESERIES", "JS_FUNCTION_DECLARATION", 
		"TIMESERIES_FUNCTION_DECLARATION", "NUM", "DOUBLE_QUOTE_STRING", "SINGLE_QUOTE_STRING", 
		"WORD", "WS", "TS_METHOD", "TS_OP_C", "TS_CL_C", "TS_OP_PAR", "TS_CL_PAR", 
		"TS_OP_Q", "TS_CL_Q", "TS_DOT", "TS_COMMA", "TS_DOL", "TS_MATH", "TS_OR", 
		"TS_TRUE", "TS_NOT", "TS_AS", "TS_AND", "TS_FROM", "TS_WHERE", "TS_GROUPBY", 
		"TS_BETWEEN", "TS_FIRST", "TS_LAST", "TS_WITH", "TS_TIMERANGE", "TS_GROUPBY_VALUE", 
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
			this.state = 187;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 0, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 184;
					this.parameterBeforeQuery();
					}
					}
				}
				this.state = 189;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 0, this._ctx);
			}
			this.state = 193;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.JS_FUNCTION_DECLARATION || _la === BaseRqlParser.TIMESERIES_FUNCTION_DECLARATION) {
				{
				{
				this.state = 190;
				this.functionStatment();
				}
				}
				this.state = 195;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 196;
			this.fromStatement();
			this.state = 198;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.GROUP_BY) {
				{
				this.state = 197;
				this.groupByStatement();
				}
			}

			this.state = 201;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.WHERE) {
				{
				this.state = 200;
				this.whereStatement();
				}
			}

			this.state = 204;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.ORDER_BY) {
				{
				this.state = 203;
				this.orderByStatement();
				}
			}

			this.state = 207;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.LOAD) {
				{
				this.state = 206;
				this.loadStatement();
				}
			}

			this.state = 210;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.UPDATE) {
				{
				this.state = 209;
				this.updateStatement();
				}
			}

			this.state = 213;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.FILTER) {
				{
				this.state = 212;
				this.filterStatement();
				}
			}

			this.state = 216;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.SELECT || _la === BaseRqlParser.JS_SELECT) {
				{
				this.state = 215;
				this.selectStatement();
				}
			}

			this.state = 219;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.INCLUDE) {
				{
				this.state = 218;
				this.includeStatement();
				}
			}

			this.state = 222;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.LIMIT || _la === BaseRqlParser.FILTER_LIMIT) {
				{
				this.state = 221;
				this.limitStatement();
				}
			}

			this.state = 225;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.OP_CUR) {
				{
				this.state = 224;
				this.json();
				}
			}

			this.state = 227;
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
			this.state = 231;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.JS_FUNCTION_DECLARATION:
				_localctx = new JavaScriptFunctionContext(_localctx);
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 229;
				this.jsFunction();
				}
				break;
			case BaseRqlParser.TIMESERIES_FUNCTION_DECLARATION:
				_localctx = new TimeSeriesFunctionContext(_localctx);
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 230;
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
			this.state = 233;
			this.match(BaseRqlParser.UPDATE);
			this.state = 234;
			this.match(BaseRqlParser.US_OP);
			this.state = 238;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.US_OP) {
				{
				{
				this.state = 235;
				this.updateBody();
				}
				}
				this.state = 240;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 241;
			this.match(BaseRqlParser.US_CL);
			this.state = 242;
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
			this.state = 244;
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
			this.state = 259;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 16, this._ctx) ) {
			case 1:
				_localctx = new CollectionByIndexContext(_localctx);
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 246;
				this.fromMode();
				this.state = 247;
				this.match(BaseRqlParser.INDEX);
				this.state = 248;
				(_localctx as CollectionByIndexContext)._collection = this.indexName();
				this.state = 250;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (((((_la - 26)) & ~0x1F) === 0 && ((1 << (_la - 26)) & ((1 << (BaseRqlParser.ALL - 26)) | (1 << (BaseRqlParser.ALPHANUMERIC - 26)) | (1 << (BaseRqlParser.AND - 26)) | (1 << (BaseRqlParser.AS - 26)) | (1 << (BaseRqlParser.BETWEEN - 26)) | (1 << (BaseRqlParser.DISTINCT - 26)) | (1 << (BaseRqlParser.DOUBLE - 26)) | (1 << (BaseRqlParser.ENDS_WITH - 26)) | (1 << (BaseRqlParser.STARTS_WITH - 26)) | (1 << (BaseRqlParser.FALSE - 26)) | (1 << (BaseRqlParser.FACET - 26)) | (1 << (BaseRqlParser.ID - 26)) | (1 << (BaseRqlParser.IN - 26)) | (1 << (BaseRqlParser.INTERSECT - 26)) | (1 << (BaseRqlParser.LONG - 26)) | (1 << (BaseRqlParser.MATCH - 26)) | (1 << (BaseRqlParser.METADATA - 26)) | (1 << (BaseRqlParser.MORELIKETHIS - 26)) | (1 << (BaseRqlParser.NOT - 26)) | (1 << (BaseRqlParser.NULL - 26)) | (1 << (BaseRqlParser.OR - 26)))) !== 0) || ((((_la - 58)) & ~0x1F) === 0 && ((1 << (_la - 58)) & ((1 << (BaseRqlParser.SORTING - 58)) | (1 << (BaseRqlParser.STRING_W - 58)) | (1 << (BaseRqlParser.TO - 58)) | (1 << (BaseRqlParser.TRUE - 58)) | (1 << (BaseRqlParser.WITH - 58)) | (1 << (BaseRqlParser.EXACT - 58)) | (1 << (BaseRqlParser.BOOST - 58)) | (1 << (BaseRqlParser.SEARCH - 58)) | (1 << (BaseRqlParser.FUZZY - 58)) | (1 << (BaseRqlParser.DOUBLE_QUOTE_STRING - 58)) | (1 << (BaseRqlParser.SINGLE_QUOTE_STRING - 58)) | (1 << (BaseRqlParser.WORD - 58)))) !== 0)) {
					{
					this.state = 249;
					this.aliasWithOptionalAs();
					}
				}

				}
				break;

			case 2:
				_localctx = new AllCollectionsContext(_localctx);
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 252;
				this.match(BaseRqlParser.FROM);
				this.state = 253;
				this.match(BaseRqlParser.ALL_DOCS);
				}
				break;

			case 3:
				_localctx = new CollectionByNameContext(_localctx);
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 254;
				this.fromMode();
				this.state = 255;
				(_localctx as CollectionByNameContext)._collection = this.collectionName();
				this.state = 257;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (((((_la - 26)) & ~0x1F) === 0 && ((1 << (_la - 26)) & ((1 << (BaseRqlParser.ALL - 26)) | (1 << (BaseRqlParser.ALPHANUMERIC - 26)) | (1 << (BaseRqlParser.AND - 26)) | (1 << (BaseRqlParser.AS - 26)) | (1 << (BaseRqlParser.BETWEEN - 26)) | (1 << (BaseRqlParser.DISTINCT - 26)) | (1 << (BaseRqlParser.DOUBLE - 26)) | (1 << (BaseRqlParser.ENDS_WITH - 26)) | (1 << (BaseRqlParser.STARTS_WITH - 26)) | (1 << (BaseRqlParser.FALSE - 26)) | (1 << (BaseRqlParser.FACET - 26)) | (1 << (BaseRqlParser.ID - 26)) | (1 << (BaseRqlParser.IN - 26)) | (1 << (BaseRqlParser.INTERSECT - 26)) | (1 << (BaseRqlParser.LONG - 26)) | (1 << (BaseRqlParser.MATCH - 26)) | (1 << (BaseRqlParser.METADATA - 26)) | (1 << (BaseRqlParser.MORELIKETHIS - 26)) | (1 << (BaseRqlParser.NOT - 26)) | (1 << (BaseRqlParser.NULL - 26)) | (1 << (BaseRqlParser.OR - 26)))) !== 0) || ((((_la - 58)) & ~0x1F) === 0 && ((1 << (_la - 58)) & ((1 << (BaseRqlParser.SORTING - 58)) | (1 << (BaseRqlParser.STRING_W - 58)) | (1 << (BaseRqlParser.TO - 58)) | (1 << (BaseRqlParser.TRUE - 58)) | (1 << (BaseRqlParser.WITH - 58)) | (1 << (BaseRqlParser.EXACT - 58)) | (1 << (BaseRqlParser.BOOST - 58)) | (1 << (BaseRqlParser.SEARCH - 58)) | (1 << (BaseRqlParser.FUZZY - 58)) | (1 << (BaseRqlParser.DOUBLE_QUOTE_STRING - 58)) | (1 << (BaseRqlParser.SINGLE_QUOTE_STRING - 58)) | (1 << (BaseRqlParser.WORD - 58)))) !== 0)) {
					{
					this.state = 256;
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
			this.state = 264;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.WORD:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 261;
				this.match(BaseRqlParser.WORD);
				}
				break;
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 262;
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
				this.state = 263;
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
			this.state = 269;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.WORD:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 266;
				this.match(BaseRqlParser.WORD);
				}
				break;
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 267;
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
				this.state = 268;
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
			this.state = 272;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.AS) {
				{
				this.state = 271;
				this.match(BaseRqlParser.AS);
				}
			}

			this.state = 274;
			_localctx._name = this.aliasName();
			this.state = 276;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.OP_Q) {
				{
				this.state = 275;
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
			this.state = 278;
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
			this.state = 280;
			this.groupByMode();
			{
			this.state = 281;
			_localctx._value = this.parameterWithOptionalAlias();
			}
			this.state = 286;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 282;
				this.match(BaseRqlParser.COMMA);
				{
				this.state = 283;
				this.parameterWithOptionalAlias();
				}
				}
				}
				this.state = 288;
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
			this.state = 291;
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
			this.state = 293;
			this.whereMode();
			this.state = 294;
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
			this.state = 319;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 23, this._ctx) ) {
			case 1:
				{
				_localctx = new OpParContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;

				this.state = 297;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 298;
				this.expr(0);
				this.state = 299;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;

			case 2:
				{
				_localctx = new EqualExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 301;
				(_localctx as EqualExpressionContext)._left = this.exprValue();
				this.state = 302;
				this.match(BaseRqlParser.EQUAL);
				this.state = 303;
				(_localctx as EqualExpressionContext)._right = this.exprValue();
				}
				break;

			case 3:
				{
				_localctx = new MathExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 305;
				(_localctx as MathExpressionContext)._left = this.exprValue();
				this.state = 306;
				this.match(BaseRqlParser.MATH);
				this.state = 307;
				(_localctx as MathExpressionContext)._right = this.exprValue();
				}
				break;

			case 4:
				{
				_localctx = new SpecialFunctionstContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 309;
				this.specialFunctions();
				}
				break;

			case 5:
				{
				_localctx = new InExprContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 310;
				this.inFunction();
				}
				break;

			case 6:
				{
				_localctx = new BetweenExprContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 311;
				this.betweenFunction();
				}
				break;

			case 7:
				{
				_localctx = new NormalFuncContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 312;
				(_localctx as NormalFuncContext)._funcExpr = this.function();
				}
				break;

			case 8:
				{
				_localctx = new BooleanExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 313;
				this.match(BaseRqlParser.TRUE);
				this.state = 314;
				this.match(BaseRqlParser.AND);
				this.state = 316;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 22, this._ctx) ) {
				case 1:
					{
					this.state = 315;
					this.match(BaseRqlParser.NOT);
					}
					break;
				}
				this.state = 318;
				this.expr(1);
				}
				break;
			}
			this._ctx._stop = this._input.tryLT(-1);
			this.state = 327;
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
					this.state = 321;
					if (!(this.precpred(this._ctx, 9))) {
						throw this.createFailedPredicateException("this.precpred(this._ctx, 9)");
					}
					this.state = 322;
					this.binary();
					this.state = 323;
					(_localctx as BinaryExpressionContext)._right = this.expr(10);
					}
					}
				}
				this.state = 329;
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
			this.state = 336;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 25, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 330;
				this.match(BaseRqlParser.AND);
				this.state = 331;
				this.match(BaseRqlParser.NOT);
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 332;
				this.match(BaseRqlParser.OR);
				this.state = 333;
				this.match(BaseRqlParser.NOT);
				}
				break;

			case 3:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 334;
				this.match(BaseRqlParser.AND);
				}
				break;

			case 4:
				this.enterOuterAlt(_localctx, 4);
				{
				this.state = 335;
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
			this.state = 338;
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
			this.state = 340;
			_localctx._value = this.literal();
			this.state = 342;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.ALL) {
				{
				this.state = 341;
				this.match(BaseRqlParser.ALL);
				}
			}

			this.state = 344;
			this.match(BaseRqlParser.IN);
			this.state = 345;
			this.match(BaseRqlParser.OP_PAR);
			this.state = 346;
			_localctx._first = this.literal();
			this.state = 351;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 347;
				this.match(BaseRqlParser.COMMA);
				this.state = 348;
				_localctx._next = this.literal();
				}
				}
				this.state = 353;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 354;
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
			this.state = 356;
			_localctx._value = this.literal();
			this.state = 357;
			this.match(BaseRqlParser.BETWEEN);
			this.state = 358;
			_localctx._from = this.literal();
			this.state = 359;
			this.match(BaseRqlParser.AND);
			this.state = 360;
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
			this.state = 362;
			this.specialFunctionName();
			this.state = 363;
			this.match(BaseRqlParser.OP_PAR);
			this.state = 378;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << BaseRqlParser.OP_PAR) | (1 << BaseRqlParser.OP_Q) | (1 << BaseRqlParser.DOL) | (1 << BaseRqlParser.ALL) | (1 << BaseRqlParser.ALPHANUMERIC) | (1 << BaseRqlParser.AND) | (1 << BaseRqlParser.BETWEEN))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (BaseRqlParser.DISTINCT - 32)) | (1 << (BaseRqlParser.DOUBLE - 32)) | (1 << (BaseRqlParser.ENDS_WITH - 32)) | (1 << (BaseRqlParser.STARTS_WITH - 32)) | (1 << (BaseRqlParser.FALSE - 32)) | (1 << (BaseRqlParser.FACET - 32)) | (1 << (BaseRqlParser.FROM - 32)) | (1 << (BaseRqlParser.GROUP_BY - 32)) | (1 << (BaseRqlParser.ID - 32)) | (1 << (BaseRqlParser.IN - 32)) | (1 << (BaseRqlParser.INCLUDE - 32)) | (1 << (BaseRqlParser.INDEX - 32)) | (1 << (BaseRqlParser.INTERSECT - 32)) | (1 << (BaseRqlParser.LOAD - 32)) | (1 << (BaseRqlParser.LONG - 32)) | (1 << (BaseRqlParser.MATCH - 32)) | (1 << (BaseRqlParser.METADATA - 32)) | (1 << (BaseRqlParser.MORELIKETHIS - 32)) | (1 << (BaseRqlParser.NOT - 32)) | (1 << (BaseRqlParser.NULL - 32)) | (1 << (BaseRqlParser.OR - 32)) | (1 << (BaseRqlParser.ORDER_BY - 32)) | (1 << (BaseRqlParser.SELECT - 32)) | (1 << (BaseRqlParser.SORTING - 32)) | (1 << (BaseRqlParser.STRING_W - 32)) | (1 << (BaseRqlParser.TO - 32)) | (1 << (BaseRqlParser.TRUE - 32)) | (1 << (BaseRqlParser.WHERE - 32)) | (1 << (BaseRqlParser.WITH - 32)))) !== 0) || ((((_la - 64)) & ~0x1F) === 0 && ((1 << (_la - 64)) & ((1 << (BaseRqlParser.EXACT - 64)) | (1 << (BaseRqlParser.BOOST - 64)) | (1 << (BaseRqlParser.SEARCH - 64)) | (1 << (BaseRqlParser.LIMIT - 64)) | (1 << (BaseRqlParser.FUZZY - 64)) | (1 << (BaseRqlParser.FILTER - 64)) | (1 << (BaseRqlParser.FILTER_LIMIT - 64)) | (1 << (BaseRqlParser.NUM - 64)) | (1 << (BaseRqlParser.DOUBLE_QUOTE_STRING - 64)) | (1 << (BaseRqlParser.SINGLE_QUOTE_STRING - 64)) | (1 << (BaseRqlParser.WORD - 64)))) !== 0)) {
				{
				this.state = 364;
				this.specialParam(0);
				this.state = 366;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === BaseRqlParser.AS) {
					{
					this.state = 365;
					this.aliasWithRequiredAs();
					}
				}

				this.state = 375;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === BaseRqlParser.COMMA) {
					{
					{
					this.state = 368;
					this.match(BaseRqlParser.COMMA);
					this.state = 369;
					this.specialParam(0);
					this.state = 371;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la === BaseRqlParser.AS) {
						{
						this.state = 370;
						this.aliasWithRequiredAs();
						}
					}

					}
					}
					this.state = 377;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				}
			}

			this.state = 380;
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
			this.state = 382;
			_la = this._input.LA(1);
			if (!(((((_la - 34)) & ~0x1F) === 0 && ((1 << (_la - 34)) & ((1 << (BaseRqlParser.ENDS_WITH - 34)) | (1 << (BaseRqlParser.STARTS_WITH - 34)) | (1 << (BaseRqlParser.FACET - 34)) | (1 << (BaseRqlParser.ID - 34)) | (1 << (BaseRqlParser.INTERSECT - 34)) | (1 << (BaseRqlParser.MORELIKETHIS - 34)) | (1 << (BaseRqlParser.EXACT - 34)) | (1 << (BaseRqlParser.BOOST - 34)))) !== 0) || _la === BaseRqlParser.SEARCH || _la === BaseRqlParser.FUZZY)) {
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
		let _startState: number = 40;
		this.enterRecursionRule(_localctx, 40, BaseRqlParser.RULE_specialParam, _p);
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 401;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 32, this._ctx) ) {
			case 1:
				{
				this.state = 385;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 386;
				this.specialParam(0);
				this.state = 387;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;

			case 2:
				{
				this.state = 389;
				this.variable();
				this.state = 390;
				this.match(BaseRqlParser.BETWEEN);
				this.state = 391;
				this.specialParam(12);
				}
				break;

			case 3:
				{
				this.state = 393;
				this.inFunction();
				}
				break;

			case 4:
				{
				this.state = 394;
				this.betweenFunction();
				}
				break;

			case 5:
				{
				this.state = 395;
				this.specialFunctions();
				}
				break;

			case 6:
				{
				this.state = 396;
				this.date();
				}
				break;

			case 7:
				{
				this.state = 397;
				this.function();
				}
				break;

			case 8:
				{
				this.state = 398;
				this.variable();
				}
				break;

			case 9:
				{
				this.state = 399;
				this.identifiersAllNames();
				}
				break;

			case 10:
				{
				this.state = 400;
				this.match(BaseRqlParser.NUM);
				}
				break;
			}
			this._ctx._stop = this._input.tryLT(-1);
			this.state = 417;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 34, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = _localctx;
					{
					this.state = 415;
					this._errHandler.sync(this);
					switch ( this.interpreter.adaptivePredict(this._input, 33, this._ctx) ) {
					case 1:
						{
						_localctx = new SpecialParamContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_specialParam);
						this.state = 403;
						if (!(this.precpred(this._ctx, 13))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 13)");
						}
						this.state = 404;
						this.match(BaseRqlParser.EQUAL);
						this.state = 405;
						this.specialParam(14);
						}
						break;

					case 2:
						{
						_localctx = new SpecialParamContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_specialParam);
						this.state = 406;
						if (!(this.precpred(this._ctx, 11))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 11)");
						}
						this.state = 407;
						this.match(BaseRqlParser.AND);
						this.state = 408;
						this.specialParam(12);
						}
						break;

					case 3:
						{
						_localctx = new SpecialParamContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_specialParam);
						this.state = 409;
						if (!(this.precpred(this._ctx, 10))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 10)");
						}
						this.state = 410;
						this.match(BaseRqlParser.OR);
						this.state = 411;
						this.specialParam(11);
						}
						break;

					case 4:
						{
						_localctx = new SpecialParamContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_specialParam);
						this.state = 412;
						if (!(this.precpred(this._ctx, 9))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 9)");
						}
						this.state = 413;
						this.match(BaseRqlParser.MATH);
						this.state = 414;
						this.specialParam(10);
						}
						break;
					}
					}
				}
				this.state = 419;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 34, this._ctx);
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
		this.enterRule(_localctx, 42, BaseRqlParser.RULE_loadMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 420;
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
		this.enterRule(_localctx, 44, BaseRqlParser.RULE_loadStatement);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 422;
			this.loadMode();
			this.state = 423;
			_localctx._item = this.loadDocumentByName();
			this.state = 428;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 424;
				this.match(BaseRqlParser.COMMA);
				this.state = 425;
				this.loadDocumentByName();
				}
				}
				this.state = 430;
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
		this.enterRule(_localctx, 46, BaseRqlParser.RULE_loadDocumentByName);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 431;
			_localctx._name = this.variable();
			this.state = 432;
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
		this.enterRule(_localctx, 48, BaseRqlParser.RULE_orderByMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 434;
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
		this.enterRule(_localctx, 50, BaseRqlParser.RULE_orderByStatement);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 436;
			this.orderByMode();
			this.state = 437;
			_localctx._value = this.orderByItem();
			this.state = 442;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 438;
				this.match(BaseRqlParser.COMMA);
				{
				this.state = 439;
				this.orderByItem();
				}
				}
				}
				this.state = 444;
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
		this.enterRule(_localctx, 52, BaseRqlParser.RULE_orderByItem);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 445;
			_localctx._value = this.literal();
			this.state = 447;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.AS) {
				{
				this.state = 446;
				_localctx._order = this.orderBySorting();
				}
			}

			this.state = 450;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.SORTING) {
				{
				this.state = 449;
				_localctx._orderValueType = this.orderByOrder();
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
		this.enterRule(_localctx, 54, BaseRqlParser.RULE_orderBySorting);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 452;
			this.match(BaseRqlParser.AS);
			this.state = 453;
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
		this.enterRule(_localctx, 56, BaseRqlParser.RULE_orderBySortingAs);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 455;
			_la = this._input.LA(1);
			if (!(((((_la - 28)) & ~0x1F) === 0 && ((1 << (_la - 28)) & ((1 << (BaseRqlParser.ALPHANUMERIC - 28)) | (1 << (BaseRqlParser.DOUBLE - 28)) | (1 << (BaseRqlParser.LONG - 28)) | (1 << (BaseRqlParser.STRING_W - 28)))) !== 0))) {
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
		this.enterRule(_localctx, 58, BaseRqlParser.RULE_orderByOrder);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 457;
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
	public selectMode(): SelectModeContext {
		let _localctx: SelectModeContext = new SelectModeContext(this._ctx, this.state);
		this.enterRule(_localctx, 60, BaseRqlParser.RULE_selectMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 459;
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
		this.enterRule(_localctx, 62, BaseRqlParser.RULE_selectStatement);
		let _la: number;
		try {
			this.state = 493;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 45, this._ctx) ) {
			case 1:
				_localctx = new GetAllDistinctContext(_localctx);
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 461;
				this.selectMode();
				this.state = 462;
				this.match(BaseRqlParser.DISTINCT);
				this.state = 463;
				this.match(BaseRqlParser.STAR);
				this.state = 465;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 39, this._ctx) ) {
				case 1:
					{
					this.state = 464;
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
				this.state = 467;
				this.selectMode();
				this.state = 469;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 40, this._ctx) ) {
				case 1:
					{
					this.state = 468;
					this.match(BaseRqlParser.DISTINCT);
					}
					break;
				}
				this.state = 471;
				(_localctx as ProjectIndividualFieldsContext)._field = this.projectField();
				this.state = 476;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === BaseRqlParser.COMMA) {
					{
					{
					this.state = 472;
					this.match(BaseRqlParser.COMMA);
					this.state = 473;
					this.projectField();
					}
					}
					this.state = 478;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 480;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 42, this._ctx) ) {
				case 1:
					{
					this.state = 479;
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
				this.state = 482;
				this.match(BaseRqlParser.JS_SELECT);
				this.state = 486;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === BaseRqlParser.JS_OP) {
					{
					{
					this.state = 483;
					this.jsBody();
					}
					}
					this.state = 488;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 489;
				this.match(BaseRqlParser.JS_CL);
				this.state = 491;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 44, this._ctx) ) {
				case 1:
					{
					this.state = 490;
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
		this.enterRule(_localctx, 64, BaseRqlParser.RULE_projectField);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 498;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 46, this._ctx) ) {
			case 1:
				{
				this.state = 495;
				this.literal();
				}
				break;

			case 2:
				{
				this.state = 496;
				this.specialFunctions();
				}
				break;

			case 3:
				{
				this.state = 497;
				this.tsProg();
				}
				break;
			}
			this.state = 501;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.AS) {
				{
				this.state = 500;
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
		this.enterRule(_localctx, 66, BaseRqlParser.RULE_jsFunction);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 503;
			this.match(BaseRqlParser.JS_FUNCTION_DECLARATION);
			this.state = 504;
			this.match(BaseRqlParser.JFN_WORD);
			this.state = 505;
			this.match(BaseRqlParser.JFN_OP_PAR);
			this.state = 507;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.JFN_WORD) {
				{
				this.state = 506;
				this.match(BaseRqlParser.JFN_WORD);
				}
			}

			this.state = 513;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.JFN_COMMA) {
				{
				{
				this.state = 509;
				this.match(BaseRqlParser.JFN_COMMA);
				this.state = 510;
				this.match(BaseRqlParser.JFN_WORD);
				}
				}
				this.state = 515;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 516;
			this.match(BaseRqlParser.JFN_CL_PAR);
			this.state = 517;
			this.match(BaseRqlParser.JFN_OP_JS);
			this.state = 521;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.JS_OP) {
				{
				{
				this.state = 518;
				this.jsBody();
				}
				}
				this.state = 523;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 524;
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
		this.enterRule(_localctx, 68, BaseRqlParser.RULE_jsBody);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 526;
			this.match(BaseRqlParser.JS_OP);
			this.state = 530;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.JS_OP) {
				{
				{
				this.state = 527;
				this.jsBody();
				}
				}
				this.state = 532;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 533;
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
		this.enterRule(_localctx, 70, BaseRqlParser.RULE_aliasWithRequiredAs);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 535;
			this.match(BaseRqlParser.AS);
			this.state = 536;
			_localctx._name = this.aliasName();
			this.state = 538;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.OP_Q) {
				{
				this.state = 537;
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
		this.enterRule(_localctx, 72, BaseRqlParser.RULE_aliasName);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 543;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.WORD:
				{
				this.state = 540;
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
				this.state = 541;
				this.identifiersWithoutRootKeywords();
				}
				break;
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
				{
				this.state = 542;
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
		this.enterRule(_localctx, 74, BaseRqlParser.RULE_prealias);
		let _la: number;
		try {
			this.state = 559;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.METADATA:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 545;
				this.match(BaseRqlParser.METADATA);
				this.state = 546;
				this.match(BaseRqlParser.DOT);
				}
				break;
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
			case BaseRqlParser.WORD:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 555;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				do {
					{
					{
					this.state = 549;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case BaseRqlParser.WORD:
						{
						this.state = 547;
						this.match(BaseRqlParser.WORD);
						}
						break;
					case BaseRqlParser.DOUBLE_QUOTE_STRING:
					case BaseRqlParser.SINGLE_QUOTE_STRING:
						{
						this.state = 548;
						this.string();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					this.state = 552;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la === BaseRqlParser.OP_Q) {
						{
						this.state = 551;
						this.asArray();
						}
					}

					this.state = 554;
					this.match(BaseRqlParser.DOT);
					}
					}
					this.state = 557;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				} while (((((_la - 75)) & ~0x1F) === 0 && ((1 << (_la - 75)) & ((1 << (BaseRqlParser.DOUBLE_QUOTE_STRING - 75)) | (1 << (BaseRqlParser.SINGLE_QUOTE_STRING - 75)) | (1 << (BaseRqlParser.WORD - 75)))) !== 0));
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
		this.enterRule(_localctx, 76, BaseRqlParser.RULE_asArray);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 561;
			this.match(BaseRqlParser.OP_Q);
			this.state = 562;
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
		this.enterRule(_localctx, 78, BaseRqlParser.RULE_includeMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 564;
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
		this.enterRule(_localctx, 80, BaseRqlParser.RULE_includeStatement);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 566;
			this.includeMode();
			this.state = 569;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TIMESERIES:
				{
				this.state = 567;
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
				this.state = 568;
				this.literal();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 578;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 571;
				this.match(BaseRqlParser.COMMA);
				this.state = 574;
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
					this.state = 572;
					this.literal();
					}
					break;
				case BaseRqlParser.TIMESERIES:
					{
					this.state = 573;
					this.tsIncludeTimeseriesFunction();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				this.state = 580;
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
		this.enterRule(_localctx, 82, BaseRqlParser.RULE_limitStatement);
		let _la: number;
		try {
			this.state = 593;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.LIMIT:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 581;
				this.match(BaseRqlParser.LIMIT);
				this.state = 582;
				this.variable();
				this.state = 585;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === BaseRqlParser.COMMA || _la === BaseRqlParser.OFFSET) {
					{
					this.state = 583;
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
					this.state = 584;
					this.variable();
					}
				}

				this.state = 589;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 62, this._ctx) ) {
				case 1:
					{
					this.state = 587;
					this.match(BaseRqlParser.FILTER_LIMIT);
					this.state = 588;
					this.variable();
					}
					break;
				}
				}
				break;
			case BaseRqlParser.FILTER_LIMIT:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 591;
				this.match(BaseRqlParser.FILTER_LIMIT);
				this.state = 592;
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
		this.enterRule(_localctx, 84, BaseRqlParser.RULE_variable);
		try {
			this.state = 600;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 64, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 595;
				_localctx._name = this.memberName();
				this.state = 596;
				this.match(BaseRqlParser.DOT);
				this.state = 597;
				_localctx._member = this.variable();
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 599;
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
		this.enterRule(_localctx, 86, BaseRqlParser.RULE_memberName);
		try {
			this.state = 604;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.DOL:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 602;
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
				this.state = 603;
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
		this.enterRule(_localctx, 88, BaseRqlParser.RULE_param);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 614;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 66, this._ctx) ) {
			case 1:
				{
				this.state = 606;
				this.match(BaseRqlParser.NUM);
				}
				break;

			case 2:
				{
				this.state = 607;
				this.match(BaseRqlParser.WORD);
				}
				break;

			case 3:
				{
				this.state = 608;
				this.date();
				}
				break;

			case 4:
				{
				this.state = 609;
				this.string();
				}
				break;

			case 5:
				{
				this.state = 610;
				this.match(BaseRqlParser.ID);
				this.state = 611;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 612;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;

			case 6:
				{
				this.state = 613;
				this.identifiersAllNames();
				}
				break;
			}
			this.state = 617;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 67, this._ctx) ) {
			case 1:
				{
				this.state = 616;
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
		this.enterRule(_localctx, 90, BaseRqlParser.RULE_literal);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 620;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 68, this._ctx) ) {
			case 1:
				{
				this.state = 619;
				this.match(BaseRqlParser.DOL);
				}
				break;
			}
			this.state = 624;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 69, this._ctx) ) {
			case 1:
				{
				this.state = 622;
				this.function();
				}
				break;

			case 2:
				{
				this.state = 623;
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
		this.enterRule(_localctx, 92, BaseRqlParser.RULE_cacheParam);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 626;
			this.match(BaseRqlParser.DOL);
			this.state = 627;
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
		this.enterRule(_localctx, 94, BaseRqlParser.RULE_parameterWithOptionalAlias);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 629;
			_localctx._value = this.variableOrFunction();
			this.state = 631;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.AS) {
				{
				this.state = 630;
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
		this.enterRule(_localctx, 96, BaseRqlParser.RULE_variableOrFunction);
		try {
			this.state = 635;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 71, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 633;
				this.variable();
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 634;
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
		this.enterRule(_localctx, 98, BaseRqlParser.RULE_function);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 637;
			_localctx._addr = this.variable();
			this.state = 638;
			this.match(BaseRqlParser.OP_PAR);
			this.state = 640;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << BaseRqlParser.OP_Q) | (1 << BaseRqlParser.DOL) | (1 << BaseRqlParser.ALL) | (1 << BaseRqlParser.ALPHANUMERIC) | (1 << BaseRqlParser.AND) | (1 << BaseRqlParser.BETWEEN))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (BaseRqlParser.DISTINCT - 32)) | (1 << (BaseRqlParser.DOUBLE - 32)) | (1 << (BaseRqlParser.ENDS_WITH - 32)) | (1 << (BaseRqlParser.STARTS_WITH - 32)) | (1 << (BaseRqlParser.FALSE - 32)) | (1 << (BaseRqlParser.FACET - 32)) | (1 << (BaseRqlParser.FROM - 32)) | (1 << (BaseRqlParser.GROUP_BY - 32)) | (1 << (BaseRqlParser.ID - 32)) | (1 << (BaseRqlParser.IN - 32)) | (1 << (BaseRqlParser.INCLUDE - 32)) | (1 << (BaseRqlParser.INDEX - 32)) | (1 << (BaseRqlParser.INTERSECT - 32)) | (1 << (BaseRqlParser.LOAD - 32)) | (1 << (BaseRqlParser.LONG - 32)) | (1 << (BaseRqlParser.MATCH - 32)) | (1 << (BaseRqlParser.METADATA - 32)) | (1 << (BaseRqlParser.MORELIKETHIS - 32)) | (1 << (BaseRqlParser.NOT - 32)) | (1 << (BaseRqlParser.NULL - 32)) | (1 << (BaseRqlParser.OR - 32)) | (1 << (BaseRqlParser.ORDER_BY - 32)) | (1 << (BaseRqlParser.SELECT - 32)) | (1 << (BaseRqlParser.SORTING - 32)) | (1 << (BaseRqlParser.STRING_W - 32)) | (1 << (BaseRqlParser.TO - 32)) | (1 << (BaseRqlParser.TRUE - 32)) | (1 << (BaseRqlParser.WHERE - 32)) | (1 << (BaseRqlParser.WITH - 32)))) !== 0) || ((((_la - 64)) & ~0x1F) === 0 && ((1 << (_la - 64)) & ((1 << (BaseRqlParser.EXACT - 64)) | (1 << (BaseRqlParser.BOOST - 64)) | (1 << (BaseRqlParser.SEARCH - 64)) | (1 << (BaseRqlParser.LIMIT - 64)) | (1 << (BaseRqlParser.FUZZY - 64)) | (1 << (BaseRqlParser.FILTER - 64)) | (1 << (BaseRqlParser.FILTER_LIMIT - 64)) | (1 << (BaseRqlParser.NUM - 64)) | (1 << (BaseRqlParser.DOUBLE_QUOTE_STRING - 64)) | (1 << (BaseRqlParser.SINGLE_QUOTE_STRING - 64)) | (1 << (BaseRqlParser.WORD - 64)))) !== 0)) {
				{
				this.state = 639;
				_localctx._args = this.arguments();
				}
			}

			this.state = 642;
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
		this.enterRule(_localctx, 100, BaseRqlParser.RULE_arguments);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 644;
			this.literal();
			this.state = 649;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.COMMA) {
				{
				{
				this.state = 645;
				this.match(BaseRqlParser.COMMA);
				this.state = 646;
				this.literal();
				}
				}
				this.state = 651;
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
		this.enterRule(_localctx, 102, BaseRqlParser.RULE_identifiersWithoutRootKeywords);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 652;
			_la = this._input.LA(1);
			if (!(((((_la - 26)) & ~0x1F) === 0 && ((1 << (_la - 26)) & ((1 << (BaseRqlParser.ALL - 26)) | (1 << (BaseRqlParser.ALPHANUMERIC - 26)) | (1 << (BaseRqlParser.AND - 26)) | (1 << (BaseRqlParser.BETWEEN - 26)) | (1 << (BaseRqlParser.DISTINCT - 26)) | (1 << (BaseRqlParser.DOUBLE - 26)) | (1 << (BaseRqlParser.ENDS_WITH - 26)) | (1 << (BaseRqlParser.STARTS_WITH - 26)) | (1 << (BaseRqlParser.FALSE - 26)) | (1 << (BaseRqlParser.FACET - 26)) | (1 << (BaseRqlParser.ID - 26)) | (1 << (BaseRqlParser.IN - 26)) | (1 << (BaseRqlParser.INTERSECT - 26)) | (1 << (BaseRqlParser.LONG - 26)) | (1 << (BaseRqlParser.MATCH - 26)) | (1 << (BaseRqlParser.METADATA - 26)) | (1 << (BaseRqlParser.MORELIKETHIS - 26)) | (1 << (BaseRqlParser.NOT - 26)) | (1 << (BaseRqlParser.NULL - 26)) | (1 << (BaseRqlParser.OR - 26)))) !== 0) || ((((_la - 58)) & ~0x1F) === 0 && ((1 << (_la - 58)) & ((1 << (BaseRqlParser.SORTING - 58)) | (1 << (BaseRqlParser.STRING_W - 58)) | (1 << (BaseRqlParser.TO - 58)) | (1 << (BaseRqlParser.TRUE - 58)) | (1 << (BaseRqlParser.WITH - 58)) | (1 << (BaseRqlParser.EXACT - 58)) | (1 << (BaseRqlParser.BOOST - 58)) | (1 << (BaseRqlParser.SEARCH - 58)) | (1 << (BaseRqlParser.FUZZY - 58)))) !== 0))) {
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
		this.enterRule(_localctx, 104, BaseRqlParser.RULE_rootKeywords);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 654;
			_la = this._input.LA(1);
			if (!(((((_la - 38)) & ~0x1F) === 0 && ((1 << (_la - 38)) & ((1 << (BaseRqlParser.FROM - 38)) | (1 << (BaseRqlParser.GROUP_BY - 38)) | (1 << (BaseRqlParser.INCLUDE - 38)) | (1 << (BaseRqlParser.INDEX - 38)) | (1 << (BaseRqlParser.LOAD - 38)) | (1 << (BaseRqlParser.ORDER_BY - 38)) | (1 << (BaseRqlParser.SELECT - 38)) | (1 << (BaseRqlParser.WHERE - 38)) | (1 << (BaseRqlParser.LIMIT - 38)) | (1 << (BaseRqlParser.FILTER - 38)))) !== 0) || _la === BaseRqlParser.FILTER_LIMIT)) {
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
		this.enterRule(_localctx, 106, BaseRqlParser.RULE_identifiersAllNames);
		try {
			this.state = 658;
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
				this.state = 656;
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
				this.state = 657;
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
		this.enterRule(_localctx, 108, BaseRqlParser.RULE_date);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 660;
			this.match(BaseRqlParser.OP_Q);
			this.state = 663;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.NULL:
				{
				this.state = 661;
				this.match(BaseRqlParser.NULL);
				}
				break;
			case BaseRqlParser.WORD:
				{
				this.state = 662;
				this.dateString();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 665;
			this.match(BaseRqlParser.TO);
			this.state = 668;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.NULL:
				{
				this.state = 666;
				this.match(BaseRqlParser.NULL);
				}
				break;
			case BaseRqlParser.WORD:
				{
				this.state = 667;
				this.dateString();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 670;
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
		this.enterRule(_localctx, 110, BaseRqlParser.RULE_dateString);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 672;
			this.match(BaseRqlParser.WORD);
			this.state = 673;
			this.match(BaseRqlParser.DOT);
			this.state = 674;
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
		this.enterRule(_localctx, 112, BaseRqlParser.RULE_tsProg);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 676;
			this.match(BaseRqlParser.TIMESERIES);
			this.state = 677;
			this.tsQueryBody();
			this.state = 678;
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
		this.enterRule(_localctx, 114, BaseRqlParser.RULE_tsIncludeTimeseriesFunction);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 680;
			this.match(BaseRqlParser.TIMESERIES);
			this.state = 681;
			this.tsLiteral();
			this.state = 684;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 77, this._ctx) ) {
			case 1:
				{
				this.state = 682;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 683;
				this.tsIncludeLiteral();
				}
				break;
			}
			this.state = 688;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_COMMA) {
				{
				this.state = 686;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 687;
				this.tsIncludeLiteral();
				}
			}

			this.state = 690;
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
		this.enterRule(_localctx, 116, BaseRqlParser.RULE_tsIncludeLiteral);
		try {
			this.state = 694;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TS_DOL:
			case BaseRqlParser.TS_STRING:
			case BaseRqlParser.TS_WORD:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 692;
				this.tsLiteral();
				}
				break;
			case BaseRqlParser.TS_FIRST:
			case BaseRqlParser.TS_LAST:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 693;
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
		this.enterRule(_localctx, 118, BaseRqlParser.RULE_tsIncludeSpecialMethod);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 696;
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
			this.state = 697;
			this.match(BaseRqlParser.TS_OP_PAR);
			this.state = 698;
			this.match(BaseRqlParser.TS_NUM);
			this.state = 701;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_COMMA) {
				{
				this.state = 699;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 700;
				this.match(BaseRqlParser.TS_STRING);
				}
			}

			this.state = 703;
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
		this.enterRule(_localctx, 120, BaseRqlParser.RULE_tsQueryBody);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 705;
			_localctx._from = this.tsFROM();
			this.state = 707;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 98)) & ~0x1F) === 0 && ((1 << (_la - 98)) & ((1 << (BaseRqlParser.TS_BETWEEN - 98)) | (1 << (BaseRqlParser.TS_FIRST - 98)) | (1 << (BaseRqlParser.TS_LAST - 98)))) !== 0)) {
				{
				this.state = 706;
				_localctx._range = this.tsTimeRangeStatement();
				}
			}

			this.state = 710;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_LOAD) {
				{
				this.state = 709;
				_localctx._load = this.tsLoadStatement();
				}
			}

			this.state = 713;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_WHERE) {
				{
				this.state = 712;
				_localctx._where = this.tsWHERE();
				}
			}

			this.state = 716;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_GROUPBY) {
				{
				this.state = 715;
				_localctx._groupBy = this.tsGroupBy();
				}
			}

			this.state = 719;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_SELECT) {
				{
				this.state = 718;
				_localctx._select = this.tsSelect();
				}
			}

			this.state = 722;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_SCALE) {
				{
				this.state = 721;
				_localctx._scale = this.tsSelectScaleProjection();
				}
			}

			this.state = 725;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_OFFSET) {
				{
				this.state = 724;
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
		this.enterRule(_localctx, 122, BaseRqlParser.RULE_tsOffset);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 727;
			this.match(BaseRqlParser.TS_OFFSET);
			this.state = 728;
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
		this.enterRule(_localctx, 124, BaseRqlParser.RULE_tsFunction);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 730;
			this.match(BaseRqlParser.TIMESERIES_FUNCTION_DECLARATION);
			this.state = 731;
			this.match(BaseRqlParser.TS_WORD);
			this.state = 732;
			this.match(BaseRqlParser.TS_OP_PAR);
			this.state = 733;
			this.match(BaseRqlParser.TS_WORD);
			this.state = 738;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.TS_COMMA) {
				{
				{
				this.state = 734;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 735;
				this.match(BaseRqlParser.TS_WORD);
				}
				}
				this.state = 740;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 741;
			this.match(BaseRqlParser.TS_CL_PAR);
			this.state = 742;
			this.match(BaseRqlParser.TS_OP_C);
			this.state = 743;
			_localctx._body = this.tsQueryBody();
			this.state = 744;
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
		this.enterRule(_localctx, 126, BaseRqlParser.RULE_tsTimeRangeStatement);
		try {
			this.state = 752;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 89, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 746;
				this.tsBetween();
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 747;
				_localctx._first = this.tsTimeRangeFirst();
				this.state = 748;
				_localctx._last = this.tsTimeRangeLast();
				}
				break;

			case 3:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 750;
				_localctx._first = this.tsTimeRangeFirst();
				}
				break;

			case 4:
				this.enterOuterAlt(_localctx, 4);
				{
				this.state = 751;
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
		this.enterRule(_localctx, 128, BaseRqlParser.RULE_tsLoadStatement);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 754;
			this.match(BaseRqlParser.TS_LOAD);
			this.state = 755;
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
		this.enterRule(_localctx, 130, BaseRqlParser.RULE_tsAlias);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 757;
			this.match(BaseRqlParser.TS_AS);
			this.state = 758;
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
		this.enterRule(_localctx, 132, BaseRqlParser.RULE_tsFROM);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 760;
			this.match(BaseRqlParser.TS_FROM);
			this.state = 761;
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
		this.enterRule(_localctx, 134, BaseRqlParser.RULE_tsWHERE);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 763;
			this.match(BaseRqlParser.TS_WHERE);
			this.state = 764;
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
		let _startState: number = 136;
		this.enterRecursionRule(_localctx, 136, BaseRqlParser.RULE_tsExpr, _p);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 778;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TS_OP_PAR:
				{
				_localctx = new TsOpParContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;

				this.state = 767;
				this.match(BaseRqlParser.TS_OP_PAR);
				this.state = 768;
				this.tsExpr(0);
				this.state = 769;
				this.match(BaseRqlParser.TS_CL_PAR);
				}
				break;
			case BaseRqlParser.TS_TRUE:
				{
				_localctx = new TsBooleanExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 771;
				this.match(BaseRqlParser.TS_TRUE);
				this.state = 772;
				this.match(BaseRqlParser.TS_AND);
				this.state = 774;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === BaseRqlParser.TS_NOT) {
					{
					this.state = 773;
					this.match(BaseRqlParser.TS_NOT);
					}
				}

				this.state = 776;
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
				this.state = 777;
				this.tsLiteral();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this._ctx._stop = this._input.tryLT(-1);
			this.state = 789;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 93, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = _localctx;
					{
					this.state = 787;
					this._errHandler.sync(this);
					switch ( this.interpreter.adaptivePredict(this._input, 92, this._ctx) ) {
					case 1:
						{
						_localctx = new TsMathExpressionContext(new TsExprContext(_parentctx, _parentState));
						(_localctx as TsMathExpressionContext)._left = _prevctx;
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_tsExpr);
						this.state = 780;
						if (!(this.precpred(this._ctx, 5))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 5)");
						}
						this.state = 781;
						this.match(BaseRqlParser.TS_MATH);
						this.state = 782;
						(_localctx as TsMathExpressionContext)._right = this.tsExpr(6);
						}
						break;

					case 2:
						{
						_localctx = new TsBinaryExpressionContext(new TsExprContext(_parentctx, _parentState));
						(_localctx as TsBinaryExpressionContext)._left = _prevctx;
						this.pushNewRecursionContext(_localctx, _startState, BaseRqlParser.RULE_tsExpr);
						this.state = 783;
						if (!(this.precpred(this._ctx, 4))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 4)");
						}
						this.state = 784;
						this.tsBinary();
						this.state = 785;
						(_localctx as TsBinaryExpressionContext)._right = this.tsExpr(5);
						}
						break;
					}
					}
				}
				this.state = 791;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 93, this._ctx);
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
		this.enterRule(_localctx, 138, BaseRqlParser.RULE_tsBetween);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 792;
			this.match(BaseRqlParser.TS_BETWEEN);
			this.state = 793;
			_localctx._from = this.tsLiteral();
			this.state = 794;
			this.match(BaseRqlParser.TS_AND);
			this.state = 795;
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
		this.enterRule(_localctx, 140, BaseRqlParser.RULE_tsBinary);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 797;
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
		this.enterRule(_localctx, 142, BaseRqlParser.RULE_tsLiteral);
		let _la: number;
		try {
			let _alt: number;
			this.state = 822;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TS_DOL:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 799;
				this.match(BaseRqlParser.TS_DOL);
				this.state = 803;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case BaseRqlParser.TS_WORD:
					{
					this.state = 800;
					this.match(BaseRqlParser.TS_WORD);
					}
					break;
				case BaseRqlParser.TS_NUM:
					{
					this.state = 801;
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
					this.state = 802;
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
				this.state = 805;
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
				this.state = 809;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 95, this._ctx) ) {
				case 1:
					{
					this.state = 806;
					this.match(BaseRqlParser.TS_OP_Q);
					this.state = 807;
					this.match(BaseRqlParser.TS_NUM);
					this.state = 808;
					this.match(BaseRqlParser.TS_CL_Q);
					}
					break;
				}
				this.state = 819;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 97, this._ctx);
				while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
					if (_alt === 1) {
						{
						{
						this.state = 811;
						this.match(BaseRqlParser.TS_DOT);
						this.state = 815;
						this._errHandler.sync(this);
						switch (this._input.LA(1)) {
						case BaseRqlParser.TS_WORD:
							{
							this.state = 812;
							this.match(BaseRqlParser.TS_WORD);
							}
							break;
						case BaseRqlParser.TS_STRING:
							{
							this.state = 813;
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
							this.state = 814;
							this.tsIdentifiers();
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						}
						}
					}
					this.state = 821;
					this._errHandler.sync(this);
					_alt = this.interpreter.adaptivePredict(this._input, 97, this._ctx);
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
		this.enterRule(_localctx, 144, BaseRqlParser.RULE_tsTimeRangeFirst);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 824;
			this.match(BaseRqlParser.TS_FIRST);
			this.state = 825;
			_localctx._num = this.match(BaseRqlParser.TS_NUM);
			this.state = 826;
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
		this.enterRule(_localctx, 146, BaseRqlParser.RULE_tsTimeRangeLast);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 828;
			this.match(BaseRqlParser.TS_LAST);
			this.state = 829;
			_localctx._num = this.match(BaseRqlParser.TS_NUM);
			this.state = 830;
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
		this.enterRule(_localctx, 148, BaseRqlParser.RULE_tsCollectionName);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 835;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TS_WORD:
				{
				this.state = 832;
				this.match(BaseRqlParser.TS_WORD);
				}
				break;
			case BaseRqlParser.TS_STRING:
				{
				this.state = 833;
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
				this.state = 834;
				this.tsIdentifiers();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 839;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_DOT) {
				{
				this.state = 837;
				this.match(BaseRqlParser.TS_DOT);
				this.state = 838;
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
		this.enterRule(_localctx, 150, BaseRqlParser.RULE_tsGroupBy);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 841;
			this.match(BaseRqlParser.TS_GROUPBY);
			this.state = 842;
			_localctx._name = this.tsCollectionName();
			this.state = 847;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.TS_COMMA) {
				{
				{
				this.state = 843;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 844;
				this.tsCollectionName();
				}
				}
				this.state = 849;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 851;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === BaseRqlParser.TS_WITH) {
				{
				this.state = 850;
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
		this.enterRule(_localctx, 152, BaseRqlParser.RULE_tsSelect);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 853;
			this.match(BaseRqlParser.TS_SELECT);
			this.state = 854;
			_localctx._field = this.tsSelectVariable();
			this.state = 859;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.TS_COMMA) {
				{
				{
				this.state = 855;
				this.match(BaseRqlParser.TS_COMMA);
				this.state = 856;
				this.tsSelectVariable();
				}
				}
				this.state = 861;
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
		this.enterRule(_localctx, 154, BaseRqlParser.RULE_tsSelectScaleProjection);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 862;
			this.match(BaseRqlParser.TS_SCALE);
			this.state = 863;
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
		this.enterRule(_localctx, 156, BaseRqlParser.RULE_tsSelectVariable);
		try {
			this.state = 867;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.TS_METHOD:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 865;
				this.match(BaseRqlParser.TS_METHOD);
				}
				break;
			case BaseRqlParser.TS_DOL:
			case BaseRqlParser.TS_STRING:
			case BaseRqlParser.TS_WORD:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 866;
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
		this.enterRule(_localctx, 158, BaseRqlParser.RULE_tsIdentifiers);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 869;
			_la = this._input.LA(1);
			if (!(((((_la - 90)) & ~0x1F) === 0 && ((1 << (_la - 90)) & ((1 << (BaseRqlParser.TS_OR - 90)) | (1 << (BaseRqlParser.TS_AND - 90)) | (1 << (BaseRqlParser.TS_FROM - 90)) | (1 << (BaseRqlParser.TS_WHERE - 90)) | (1 << (BaseRqlParser.TS_GROUPBY - 90)) | (1 << (BaseRqlParser.TS_TIMERANGE - 90)))) !== 0))) {
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
		this.enterRule(_localctx, 160, BaseRqlParser.RULE_updateBody);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 871;
			this.match(BaseRqlParser.US_OP);
			this.state = 875;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === BaseRqlParser.US_OP) {
				{
				{
				this.state = 872;
				this.updateBody();
				}
				}
				this.state = 877;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 878;
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
		this.enterRule(_localctx, 162, BaseRqlParser.RULE_filterStatement);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 880;
			this.filterMode();
			this.state = 881;
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
		let _startState: number = 164;
		this.enterRecursionRule(_localctx, 164, BaseRqlParser.RULE_filterExpr, _p);
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 903;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 107, this._ctx) ) {
			case 1:
				{
				_localctx = new FilterOpParContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;

				this.state = 884;
				this.match(BaseRqlParser.OP_PAR);
				this.state = 885;
				this.filterExpr(0);
				this.state = 886;
				this.match(BaseRqlParser.CL_PAR);
				}
				break;

			case 2:
				{
				_localctx = new FilterEqualExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 888;
				(_localctx as FilterEqualExpressionContext)._left = this.exprValue();
				this.state = 889;
				this.match(BaseRqlParser.EQUAL);
				this.state = 890;
				(_localctx as FilterEqualExpressionContext)._right = this.exprValue();
				}
				break;

			case 3:
				{
				_localctx = new FilterMathExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 892;
				(_localctx as FilterMathExpressionContext)._left = this.exprValue();
				this.state = 893;
				this.match(BaseRqlParser.MATH);
				this.state = 894;
				(_localctx as FilterMathExpressionContext)._right = this.exprValue();
				}
				break;

			case 4:
				{
				_localctx = new FilterNormalFuncContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 896;
				(_localctx as FilterNormalFuncContext)._funcExpr = this.function();
				}
				break;

			case 5:
				{
				_localctx = new FilterBooleanExpressionContext(_localctx);
				this._ctx = _localctx;
				_prevctx = _localctx;
				this.state = 897;
				this.match(BaseRqlParser.TRUE);
				this.state = 898;
				this.match(BaseRqlParser.AND);
				this.state = 900;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 106, this._ctx) ) {
				case 1:
					{
					this.state = 899;
					this.match(BaseRqlParser.NOT);
					}
					break;
				}
				this.state = 902;
				this.expr(0);
				}
				break;
			}
			this._ctx._stop = this._input.tryLT(-1);
			this.state = 911;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 108, this._ctx);
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
					this.state = 905;
					if (!(this.precpred(this._ctx, 6))) {
						throw this.createFailedPredicateException("this.precpred(this._ctx, 6)");
					}
					this.state = 906;
					this.binary();
					this.state = 907;
					(_localctx as FilterBinaryExpressionContext)._right = this.filterExpr(7);
					}
					}
				}
				this.state = 913;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 108, this._ctx);
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
		this.enterRule(_localctx, 166, BaseRqlParser.RULE_filterMode);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 914;
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
		this.enterRule(_localctx, 168, BaseRqlParser.RULE_parameterBeforeQuery);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 916;
			_localctx._name = this.literal();
			this.state = 917;
			this.match(BaseRqlParser.EQUAL);
			this.state = 918;
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
		this.enterRule(_localctx, 170, BaseRqlParser.RULE_parameterValue);
		try {
			this.state = 922;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
			case BaseRqlParser.SINGLE_QUOTE_STRING:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 920;
				this.string();
				}
				break;
			case BaseRqlParser.NUM:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 921;
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
		this.enterRule(_localctx, 172, BaseRqlParser.RULE_json);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 924;
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
		this.enterRule(_localctx, 174, BaseRqlParser.RULE_jsonObj);
		let _la: number;
		try {
			this.state = 939;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 111, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 926;
				this.match(BaseRqlParser.OP_CUR);
				this.state = 927;
				this.jsonPair();
				this.state = 932;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === BaseRqlParser.COMMA) {
					{
					{
					this.state = 928;
					this.match(BaseRqlParser.COMMA);
					this.state = 929;
					this.jsonPair();
					}
					}
					this.state = 934;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 935;
				this.match(BaseRqlParser.CL_CUR);
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 937;
				this.match(BaseRqlParser.OP_CUR);
				this.state = 938;
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
		this.enterRule(_localctx, 176, BaseRqlParser.RULE_jsonPair);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 941;
			this.match(BaseRqlParser.DOUBLE_QUOTE_STRING);
			this.state = 942;
			this.match(BaseRqlParser.COLON);
			this.state = 943;
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
		this.enterRule(_localctx, 178, BaseRqlParser.RULE_jsonArr);
		let _la: number;
		try {
			this.state = 958;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 113, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 945;
				this.match(BaseRqlParser.OP_Q);
				this.state = 946;
				this.jsonValue();
				this.state = 951;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === BaseRqlParser.COMMA) {
					{
					{
					this.state = 947;
					this.match(BaseRqlParser.COMMA);
					this.state = 948;
					this.jsonValue();
					}
					}
					this.state = 953;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 954;
				this.match(BaseRqlParser.CL_Q);
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 956;
				this.match(BaseRqlParser.OP_Q);
				this.state = 957;
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
		this.enterRule(_localctx, 180, BaseRqlParser.RULE_jsonValue);
		try {
			this.state = 967;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case BaseRqlParser.DOUBLE_QUOTE_STRING:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 960;
				this.match(BaseRqlParser.DOUBLE_QUOTE_STRING);
				}
				break;
			case BaseRqlParser.NUM:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 961;
				this.match(BaseRqlParser.NUM);
				}
				break;
			case BaseRqlParser.OP_CUR:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 962;
				this.jsonObj();
				}
				break;
			case BaseRqlParser.OP_Q:
				this.enterOuterAlt(_localctx, 4);
				{
				this.state = 963;
				this.jsonArr();
				}
				break;
			case BaseRqlParser.TRUE:
				this.enterOuterAlt(_localctx, 5);
				{
				this.state = 964;
				this.match(BaseRqlParser.TRUE);
				}
				break;
			case BaseRqlParser.FALSE:
				this.enterOuterAlt(_localctx, 6);
				{
				this.state = 965;
				this.match(BaseRqlParser.FALSE);
				}
				break;
			case BaseRqlParser.NULL:
				this.enterOuterAlt(_localctx, 7);
				{
				this.state = 966;
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
		this.enterRule(_localctx, 182, BaseRqlParser.RULE_string);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 969;
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

		case 20:
			return this.specialParam_sempred(_localctx as SpecialParamContext, predIndex);

		case 68:
			return this.tsExpr_sempred(_localctx as TsExprContext, predIndex);

		case 82:
			return this.filterExpr_sempred(_localctx as FilterExprContext, predIndex);
		}
		return true;
	}
	private expr_sempred(_localctx: ExprContext, predIndex: number): boolean {
		switch (predIndex) {
		case 0:
			return this.precpred(this._ctx, 9);
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
		"\x03\uC91D\uCABA\u058D\uAFBA\u4F53\u0607\uEA8B\uC241\x03\x7F\u03CE\x04" +
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
		"X\tX\x04Y\tY\x04Z\tZ\x04[\t[\x04\\\t\\\x04]\t]\x03\x02\x07\x02\xBC\n\x02" +
		"\f\x02\x0E\x02\xBF\v\x02\x03\x02\x07\x02\xC2\n\x02\f\x02\x0E\x02\xC5\v" +
		"\x02\x03\x02\x03\x02\x05\x02\xC9\n\x02\x03\x02\x05\x02\xCC\n\x02\x03\x02" +
		"\x05\x02\xCF\n\x02\x03\x02\x05\x02\xD2\n\x02\x03\x02\x05\x02\xD5\n\x02" +
		"\x03\x02\x05\x02\xD8\n\x02\x03\x02\x05\x02\xDB\n\x02\x03\x02\x05\x02\xDE" +
		"\n\x02\x03\x02\x05\x02\xE1\n\x02\x03\x02\x05\x02\xE4\n\x02\x03\x02\x03" +
		"\x02\x03\x03\x03\x03\x05\x03\xEA\n\x03\x03\x04\x03\x04\x03\x04\x07\x04" +
		"\xEF\n\x04\f\x04\x0E\x04\xF2\v\x04\x03\x04\x03\x04\x03\x04\x03\x05\x03" +
		"\x05\x03\x06\x03\x06\x03\x06\x03\x06\x05\x06\xFD\n\x06\x03\x06\x03\x06" +
		"\x03\x06\x03\x06\x03\x06\x05\x06\u0104\n\x06\x05\x06\u0106\n\x06\x03\x07" +
		"\x03\x07\x03\x07\x05\x07\u010B\n\x07\x03\b\x03\b\x03\b\x05\b\u0110\n\b" +
		"\x03\t\x05\t\u0113\n\t\x03\t\x03\t\x05\t\u0117\n\t\x03\n\x03\n\x03\v\x03" +
		"\v\x03\v\x03\v\x07\v\u011F\n\v\f\v\x0E\v\u0122\v\v\x03\f\x03\f\x03\r\x03" +
		"\r\x03\x0E\x03\x0E\x03\x0E\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03" +
		"\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03" +
		"\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x05\x0F\u013F\n\x0F\x03\x0F" +
		"\x05\x0F\u0142\n\x0F\x03\x0F\x03\x0F\x03\x0F\x03\x0F\x07\x0F\u0148\n\x0F" +
		"\f\x0F\x0E\x0F\u014B\v\x0F\x03\x10\x03\x10\x03\x10\x03\x10\x03\x10\x03" +
		"\x10\x05\x10\u0153\n\x10\x03\x11\x03\x11\x03\x12\x03\x12\x05\x12\u0159" +
		"\n\x12\x03\x12\x03\x12\x03\x12\x03\x12\x03\x12\x07\x12\u0160\n\x12\f\x12" +
		"\x0E\x12\u0163\v\x12\x03\x12\x03\x12\x03\x13\x03\x13\x03\x13\x03\x13\x03" +
		"\x13\x03\x13\x03\x14\x03\x14\x03\x14\x03\x14\x05\x14\u0171\n\x14\x03\x14" +
		"\x03\x14\x03\x14\x05\x14\u0176\n\x14\x07\x14\u0178\n\x14\f\x14\x0E\x14" +
		"\u017B\v\x14\x05\x14\u017D\n\x14\x03\x14\x03\x14\x03\x15\x03\x15\x03\x16" +
		"\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16" +
		"\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16\x05\x16\u0194" +
		"\n\x16\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16\x03\x16" +
		"\x03\x16\x03\x16\x03\x16\x03\x16\x07\x16\u01A2\n\x16\f\x16\x0E\x16\u01A5" +
		"\v\x16\x03\x17\x03\x17\x03\x18\x03\x18\x03\x18\x03\x18\x07\x18\u01AD\n" +
		"\x18\f\x18\x0E\x18\u01B0\v\x18\x03\x19\x03\x19\x03\x19\x03\x1A\x03\x1A" +
		"\x03\x1B\x03\x1B\x03\x1B\x03\x1B\x07\x1B\u01BB\n\x1B\f\x1B\x0E\x1B\u01BE" +
		"\v\x1B\x03\x1C\x03\x1C\x05\x1C\u01C2\n\x1C\x03\x1C\x05\x1C\u01C5\n\x1C" +
		"\x03\x1D\x03\x1D\x03\x1D\x03\x1E\x03\x1E\x03\x1F\x03\x1F\x03 \x03 \x03" +
		"!\x03!\x03!\x03!\x05!\u01D4\n!\x03!\x03!\x05!\u01D8\n!\x03!\x03!\x03!" +
		"\x07!\u01DD\n!\f!\x0E!\u01E0\v!\x03!\x05!\u01E3\n!\x03!\x03!\x07!\u01E7" +
		"\n!\f!\x0E!\u01EA\v!\x03!\x03!\x05!\u01EE\n!\x05!\u01F0\n!\x03\"\x03\"" +
		"\x03\"\x05\"\u01F5\n\"\x03\"\x05\"\u01F8\n\"\x03#\x03#\x03#\x03#\x05#" +
		"\u01FE\n#\x03#\x03#\x07#\u0202\n#\f#\x0E#\u0205\v#\x03#\x03#\x03#\x07" +
		"#\u020A\n#\f#\x0E#\u020D\v#\x03#\x03#\x03$\x03$\x07$\u0213\n$\f$\x0E$" +
		"\u0216\v$\x03$\x03$\x03%\x03%\x03%\x05%\u021D\n%\x03&\x03&\x03&\x05&\u0222" +
		"\n&\x03\'\x03\'\x03\'\x03\'\x05\'\u0228\n\'\x03\'\x05\'\u022B\n\'\x03" +
		"\'\x06\'\u022E\n\'\r\'\x0E\'\u022F\x05\'\u0232\n\'\x03(\x03(\x03(\x03" +
		")\x03)\x03*\x03*\x03*\x05*\u023C\n*\x03*\x03*\x03*\x05*\u0241\n*\x07*" +
		"\u0243\n*\f*\x0E*\u0246\v*\x03+\x03+\x03+\x03+\x05+\u024C\n+\x03+\x03" +
		"+\x05+\u0250\n+\x03+\x03+\x05+\u0254\n+\x03,\x03,\x03,\x03,\x03,\x05," +
		"\u025B\n,\x03-\x03-\x05-\u025F\n-\x03.\x03.\x03.\x03.\x03.\x03.\x03.\x03" +
		".\x05.\u0269\n.\x03.\x05.\u026C\n.\x03/\x05/\u026F\n/\x03/\x03/\x05/\u0273" +
		"\n/\x030\x030\x030\x031\x031\x051\u027A\n1\x032\x032\x052\u027E\n2\x03" +
		"3\x033\x033\x053\u0283\n3\x033\x033\x034\x034\x034\x074\u028A\n4\f4\x0E" +
		"4\u028D\v4\x035\x035\x036\x036\x037\x037\x057\u0295\n7\x038\x038\x038" +
		"\x058\u029A\n8\x038\x038\x038\x058\u029F\n8\x038\x038\x039\x039\x039\x03" +
		"9\x03:\x03:\x03:\x03:\x03;\x03;\x03;\x03;\x05;\u02AF\n;\x03;\x03;\x05" +
		";\u02B3\n;\x03;\x03;\x03<\x03<\x05<\u02B9\n<\x03=\x03=\x03=\x03=\x03=" +
		"\x05=\u02C0\n=\x03=\x03=\x03>\x03>\x05>\u02C6\n>\x03>\x05>\u02C9\n>\x03" +
		">\x05>\u02CC\n>\x03>\x05>\u02CF\n>\x03>\x05>\u02D2\n>\x03>\x05>\u02D5" +
		"\n>\x03>\x05>\u02D8\n>\x03?\x03?\x03?\x03@\x03@\x03@\x03@\x03@\x03@\x07" +
		"@\u02E3\n@\f@\x0E@\u02E6\v@\x03@\x03@\x03@\x03@\x03@\x03A\x03A\x03A\x03" +
		"A\x03A\x03A\x05A\u02F3\nA\x03B\x03B\x03B\x03C\x03C\x03C\x03D\x03D\x03" +
		"D\x03E\x03E\x03E\x03F\x03F\x03F\x03F\x03F\x03F\x03F\x03F\x05F\u0309\n" +
		"F\x03F\x03F\x05F\u030D\nF\x03F\x03F\x03F\x03F\x03F\x03F\x03F\x07F\u0316" +
		"\nF\fF\x0EF\u0319\vF\x03G\x03G\x03G\x03G\x03G\x03H\x03H\x03I\x03I\x03" +
		"I\x03I\x05I\u0326\nI\x03I\x03I\x03I\x03I\x05I\u032C\nI\x03I\x03I\x03I" +
		"\x03I\x05I\u0332\nI\x07I\u0334\nI\fI\x0EI\u0337\vI\x05I\u0339\nI\x03J" +
		"\x03J\x03J\x03J\x03K\x03K\x03K\x03K\x03L\x03L\x03L\x05L\u0346\nL\x03L" +
		"\x03L\x05L\u034A\nL\x03M\x03M\x03M\x03M\x07M\u0350\nM\fM\x0EM\u0353\v" +
		"M\x03M\x05M\u0356\nM\x03N\x03N\x03N\x03N\x07N\u035C\nN\fN\x0EN\u035F\v" +
		"N\x03O\x03O\x03O\x03P\x03P\x05P\u0366\nP\x03Q\x03Q\x03R\x03R\x07R\u036C" +
		"\nR\fR\x0ER\u036F\vR\x03R\x03R\x03S\x03S\x03S\x03T\x03T\x03T\x03T\x03" +
		"T\x03T\x03T\x03T\x03T\x03T\x03T\x03T\x03T\x03T\x03T\x03T\x03T\x05T\u0387" +
		"\nT\x03T\x05T\u038A\nT\x03T\x03T\x03T\x03T\x07T\u0390\nT\fT\x0ET\u0393" +
		"\vT\x03U\x03U\x03V\x03V\x03V\x03V\x03W\x03W\x05W\u039D\nW\x03X\x03X\x03" +
		"Y\x03Y\x03Y\x03Y\x07Y\u03A5\nY\fY\x0EY\u03A8\vY\x03Y\x03Y\x03Y\x03Y\x05" +
		"Y\u03AE\nY\x03Z\x03Z\x03Z\x03Z\x03[\x03[\x03[\x03[\x07[\u03B8\n[\f[\x0E" +
		"[\u03BB\v[\x03[\x03[\x03[\x03[\x05[\u03C1\n[\x03\\\x03\\\x03\\\x03\\\x03" +
		"\\\x03\\\x03\\\x05\\\u03CA\n\\\x03]\x03]\x03]\x02\x02\x06\x1C*\x8A\xA6" +
		"^\x02\x02\x04\x02\x06\x02\b\x02\n\x02\f\x02\x0E\x02\x10\x02\x12\x02\x14" +
		"\x02\x16\x02\x18\x02\x1A\x02\x1C\x02\x1E\x02 \x02\"\x02$\x02&\x02(\x02" +
		"*\x02,\x02.\x020\x022\x024\x026\x028\x02:\x02<\x02>\x02@\x02B\x02D\x02" +
		"F\x02H\x02J\x02L\x02N\x02P\x02R\x02T\x02V\x02X\x02Z\x02\\\x02^\x02`\x02" +
		"b\x02d\x02f\x02h\x02j\x02l\x02n\x02p\x02r\x02t\x02v\x02x\x02z\x02|\x02" +
		"~\x02\x80\x02\x82\x02\x84\x02\x86\x02\x88\x02\x8A\x02\x8C\x02\x8E\x02" +
		"\x90\x02\x92\x02\x94\x02\x96\x02\x98\x02\x9A\x02\x9C\x02\x9E\x02\xA0\x02" +
		"\xA2\x02\xA4\x02\xA6\x02\xA8\x02\xAA\x02\xAC\x02\xAE\x02\xB0\x02\xB2\x02" +
		"\xB4\x02\xB6\x02\xB8\x02\x02\f\t\x02$%\'\'**//44BDFF\x06\x02\x1E\x1E#" +
		"#11==\x04\x02\x06\x0699\v\x02\x1C\x1C\x1E\x1F!\'*+//17<?ADFF\v\x02()," +
		",..0088::@@EEGH\x03\x02ef\x04\x02\\\\``\x04\x02ooqq\x05\x02\\\\`chh\x03" +
		"\x02MN\x02\u040E\x02\xBD\x03\x02\x02\x02\x04\xE9\x03\x02\x02\x02\x06\xEB" +
		"\x03\x02\x02\x02\b\xF6\x03\x02\x02\x02\n\u0105\x03\x02\x02\x02\f\u010A" +
		"\x03\x02\x02\x02\x0E\u010F\x03\x02\x02\x02\x10\u0112\x03\x02\x02\x02\x12" +
		"\u0118\x03\x02\x02\x02\x14\u011A\x03\x02\x02\x02\x16\u0123\x03\x02\x02" +
		"\x02\x18\u0125\x03\x02\x02\x02\x1A\u0127\x03\x02\x02\x02\x1C\u0141\x03" +
		"\x02\x02\x02\x1E\u0152\x03\x02\x02\x02 \u0154\x03\x02\x02\x02\"\u0156" +
		"\x03\x02\x02\x02$\u0166\x03\x02\x02\x02&\u016C\x03\x02\x02\x02(\u0180" +
		"\x03\x02\x02\x02*\u0193\x03\x02\x02\x02,\u01A6\x03\x02\x02\x02.\u01A8" +
		"\x03\x02\x02\x020\u01B1\x03\x02\x02\x022\u01B4\x03\x02\x02\x024\u01B6" +
		"\x03\x02\x02\x026\u01BF\x03\x02\x02\x028\u01C6\x03\x02\x02\x02:\u01C9" +
		"\x03\x02\x02\x02<\u01CB\x03\x02\x02\x02>\u01CD\x03\x02\x02\x02@\u01EF" +
		"\x03\x02\x02\x02B\u01F4\x03\x02\x02\x02D\u01F9\x03\x02\x02\x02F\u0210" +
		"\x03\x02\x02\x02H\u0219\x03\x02\x02\x02J\u0221\x03\x02\x02\x02L\u0231" +
		"\x03\x02\x02\x02N\u0233\x03\x02\x02\x02P\u0236\x03\x02\x02\x02R\u0238" +
		"\x03\x02\x02\x02T\u0253\x03\x02\x02\x02V\u025A\x03\x02\x02\x02X\u025E" +
		"\x03\x02\x02\x02Z\u0268\x03\x02\x02\x02\\\u026E\x03\x02\x02\x02^\u0274" +
		"\x03\x02\x02\x02`\u0277\x03\x02\x02\x02b\u027D\x03\x02\x02\x02d\u027F" +
		"\x03\x02\x02\x02f\u0286\x03\x02\x02\x02h\u028E\x03\x02\x02\x02j\u0290" +
		"\x03\x02\x02\x02l\u0294\x03\x02\x02\x02n\u0296\x03\x02\x02\x02p\u02A2" +
		"\x03\x02\x02\x02r\u02A6\x03\x02\x02\x02t\u02AA\x03\x02\x02\x02v\u02B8" +
		"\x03\x02\x02\x02x\u02BA\x03\x02\x02\x02z\u02C3\x03\x02\x02\x02|\u02D9" +
		"\x03\x02\x02\x02~\u02DC\x03\x02\x02\x02\x80\u02F2\x03\x02\x02\x02\x82" +
		"\u02F4\x03\x02\x02\x02\x84\u02F7\x03\x02\x02\x02\x86\u02FA\x03\x02\x02" +
		"\x02\x88\u02FD\x03\x02\x02\x02\x8A\u030C\x03\x02\x02\x02\x8C\u031A\x03" +
		"\x02\x02\x02\x8E\u031F\x03\x02\x02\x02\x90\u0338\x03\x02\x02\x02\x92\u033A" +
		"\x03\x02\x02\x02\x94\u033E\x03\x02\x02\x02\x96\u0345\x03\x02\x02\x02\x98" +
		"\u034B\x03\x02\x02\x02\x9A\u0357\x03\x02\x02\x02\x9C\u0360\x03\x02\x02" +
		"\x02\x9E\u0365\x03\x02\x02\x02\xA0\u0367\x03\x02\x02\x02\xA2\u0369\x03" +
		"\x02\x02\x02\xA4\u0372\x03\x02\x02\x02\xA6\u0389\x03\x02\x02\x02\xA8\u0394" +
		"\x03\x02\x02\x02\xAA\u0396\x03\x02\x02\x02\xAC\u039C\x03\x02\x02\x02\xAE" +
		"\u039E\x03\x02\x02\x02\xB0\u03AD\x03\x02\x02\x02\xB2\u03AF\x03\x02\x02" +
		"\x02\xB4\u03C0\x03\x02\x02\x02\xB6\u03C9\x03\x02\x02\x02\xB8\u03CB\x03" +
		"\x02\x02\x02\xBA\xBC\x05\xAAV\x02\xBB\xBA\x03\x02\x02\x02\xBC\xBF\x03" +
		"\x02\x02\x02\xBD\xBB\x03\x02\x02\x02\xBD\xBE\x03\x02\x02\x02\xBE\xC3\x03" +
		"\x02\x02\x02\xBF\xBD\x03\x02\x02\x02\xC0\xC2\x05\x04\x03\x02\xC1\xC0\x03" +
		"\x02\x02\x02\xC2\xC5\x03\x02\x02\x02\xC3\xC1\x03\x02\x02\x02\xC3\xC4\x03" +
		"\x02\x02\x02\xC4\xC6\x03\x02\x02\x02\xC5\xC3\x03\x02\x02\x02\xC6\xC8\x05" +
		"\n\x06\x02\xC7\xC9\x05\x14\v\x02\xC8\xC7\x03\x02\x02\x02\xC8\xC9\x03\x02" +
		"\x02\x02\xC9\xCB\x03\x02\x02\x02\xCA\xCC\x05\x1A\x0E\x02\xCB\xCA\x03\x02" +
		"\x02\x02\xCB\xCC\x03\x02\x02\x02\xCC\xCE\x03\x02\x02\x02\xCD\xCF\x054" +
		"\x1B\x02\xCE\xCD\x03\x02\x02\x02\xCE\xCF\x03\x02\x02\x02\xCF\xD1\x03\x02" +
		"\x02\x02\xD0\xD2\x05.\x18\x02\xD1\xD0\x03\x02\x02\x02\xD1\xD2\x03\x02" +
		"\x02\x02\xD2\xD4\x03\x02\x02\x02\xD3\xD5\x05\x06\x04\x02\xD4\xD3\x03\x02" +
		"\x02\x02\xD4\xD5\x03\x02\x02\x02\xD5\xD7\x03\x02\x02\x02\xD6\xD8\x05\xA4" +
		"S\x02\xD7\xD6\x03\x02\x02\x02\xD7\xD8\x03\x02\x02\x02\xD8\xDA\x03\x02" +
		"\x02\x02\xD9\xDB\x05@!\x02\xDA\xD9\x03\x02\x02\x02\xDA\xDB\x03\x02\x02" +
		"\x02\xDB\xDD\x03\x02\x02\x02\xDC\xDE\x05R*\x02\xDD\xDC\x03\x02\x02\x02" +
		"\xDD\xDE\x03\x02\x02\x02\xDE\xE0\x03\x02\x02\x02\xDF\xE1\x05T+\x02\xE0" +
		"\xDF\x03\x02\x02\x02\xE0\xE1\x03\x02\x02\x02\xE1\xE3\x03\x02\x02\x02\xE2" +
		"\xE4\x05\xAEX\x02\xE3\xE2\x03\x02\x02\x02\xE3\xE4\x03\x02\x02\x02\xE4" +
		"\xE5\x03\x02\x02\x02\xE5\xE6\x07\x02\x02\x03\xE6\x03\x03\x02\x02\x02\xE7" +
		"\xEA\x05D#\x02\xE8\xEA\x05~@\x02\xE9\xE7\x03\x02\x02\x02\xE9\xE8\x03\x02" +
		"\x02\x02\xEA\x05\x03\x02\x02\x02\xEB\xEC\x07-\x02\x02\xEC\xF0\x07s\x02" +
		"\x02\xED\xEF\x05\xA2R\x02\xEE\xED\x03\x02\x02\x02\xEF\xF2\x03\x02\x02" +
		"\x02\xF0\xEE\x03\x02\x02\x02\xF0\xF1\x03\x02\x02\x02\xF1\xF3\x03\x02\x02" +
		"\x02\xF2\xF0\x03\x02\x02\x02\xF3\xF4\x07t\x02\x02\xF4\xF5\x07\x02\x02" +
		"\x03\xF5\x07\x03\x02\x02\x02\xF6\xF7\x07(\x02\x02\xF7\t\x03\x02\x02\x02" +
		"\xF8\xF9\x05\b\x05\x02\xF9\xFA\x07.\x02\x02\xFA\xFC\x05\f\x07\x02\xFB" +
		"\xFD\x05\x10\t\x02\xFC\xFB\x03\x02\x02\x02\xFC\xFD\x03\x02\x02\x02\xFD" +
		"\u0106\x03\x02\x02\x02\xFE\xFF\x07(\x02\x02\xFF\u0106\x07\x1D\x02\x02" +
		"\u0100\u0101\x05\b\x05\x02\u0101\u0103\x05\x0E\b\x02\u0102\u0104\x05\x10" +
		"\t\x02\u0103\u0102\x03\x02\x02\x02\u0103\u0104\x03\x02\x02\x02\u0104\u0106" +
		"\x03\x02\x02\x02\u0105\xF8\x03\x02\x02\x02\u0105\xFE\x03\x02\x02\x02\u0105" +
		"\u0100\x03\x02\x02\x02\u0106\v\x03\x02\x02\x02\u0107\u010B\x07O\x02\x02" +
		"\u0108\u010B\x05\xB8]\x02\u0109\u010B\x05h5\x02\u010A\u0107\x03\x02\x02" +
		"\x02\u010A\u0108\x03\x02\x02\x02\u010A\u0109\x03\x02\x02\x02\u010B\r\x03" +
		"\x02\x02\x02\u010C\u0110\x07O\x02\x02\u010D\u0110\x05\xB8]\x02\u010E\u0110" +
		"\x05h5\x02\u010F\u010C\x03\x02\x02\x02\u010F\u010D\x03\x02\x02\x02\u010F" +
		"\u010E\x03\x02\x02\x02\u0110\x0F\x03\x02\x02\x02\u0111\u0113\x07 \x02" +
		"\x02\u0112\u0111\x03\x02\x02\x02\u0112\u0113\x03\x02\x02\x02\u0113\u0114" +
		"\x03\x02\x02\x02\u0114\u0116\x05J&\x02\u0115\u0117\x05N(\x02\u0116\u0115" +
		"\x03\x02\x02\x02\u0116\u0117\x03\x02\x02\x02\u0117\x11\x03\x02\x02\x02" +
		"\u0118\u0119\x07)\x02\x02\u0119\x13\x03\x02\x02\x02\u011A\u011B\x05\x12" +
		"\n\x02\u011B\u0120\x05`1\x02\u011C\u011D\x07\x06\x02\x02\u011D\u011F\x05" +
		"`1\x02\u011E\u011C\x03\x02\x02\x02\u011F\u0122\x03\x02\x02\x02\u0120\u011E" +
		"\x03\x02\x02\x02\u0120\u0121\x03\x02\x02\x02\u0121\x15\x03\x02\x02\x02" +
		"\u0122\u0120\x03\x02\x02\x02\u0123\u0124\x03\x02\x02\x02\u0124\x17\x03" +
		"\x02\x02\x02\u0125\u0126\x07@\x02\x02\u0126\x19\x03\x02\x02\x02\u0127" +
		"\u0128\x05\x18\r\x02\u0128\u0129\x05\x1C\x0F\x02\u0129\x1B\x03\x02\x02" +
		"\x02\u012A\u012B\b\x0F\x01\x02\u012B\u012C\x07\v\x02\x02\u012C\u012D\x05" +
		"\x1C\x0F\x02\u012D\u012E\x07\x04\x02\x02\u012E\u0142\x03\x02\x02\x02\u012F" +
		"\u0130\x05 \x11\x02\u0130\u0131\x07\b\x02\x02\u0131\u0132\x05 \x11\x02" +
		"\u0132\u0142\x03\x02\x02\x02\u0133\u0134\x05 \x11\x02\u0134\u0135\x07" +
		"\t\x02\x02\u0135\u0136\x05 \x11\x02\u0136\u0142\x03\x02\x02\x02\u0137" +
		"\u0142\x05&\x14\x02\u0138\u0142\x05\"\x12\x02\u0139\u0142\x05$\x13\x02" +
		"\u013A\u0142\x05d3\x02\u013B\u013C\x07?\x02\x02\u013C\u013E\x07\x1F\x02" +
		"\x02\u013D\u013F\x075\x02\x02\u013E\u013D\x03\x02\x02\x02\u013E\u013F" +
		"\x03\x02\x02\x02\u013F\u0140\x03\x02\x02\x02\u0140\u0142\x05\x1C\x0F\x03" +
		"\u0141\u012A\x03\x02\x02\x02\u0141\u012F\x03\x02\x02\x02\u0141\u0133\x03" +
		"\x02\x02\x02\u0141\u0137\x03\x02\x02\x02\u0141\u0138\x03\x02\x02\x02\u0141" +
		"\u0139\x03\x02\x02\x02\u0141\u013A\x03\x02\x02\x02\u0141\u013B\x03\x02" +
		"\x02\x02\u0142\u0149\x03\x02\x02\x02\u0143\u0144\f\v\x02\x02\u0144\u0145" +
		"\x05\x1E\x10\x02\u0145\u0146\x05\x1C\x0F\f\u0146\u0148\x03\x02\x02\x02" +
		"\u0147\u0143\x03\x02\x02\x02\u0148\u014B\x03\x02\x02\x02\u0149\u0147\x03" +
		"\x02\x02\x02\u0149\u014A\x03\x02\x02\x02\u014A\x1D\x03\x02\x02\x02\u014B" +
		"\u0149\x03\x02\x02\x02\u014C\u014D\x07\x1F\x02\x02\u014D\u0153\x075\x02" +
		"\x02\u014E\u014F\x077\x02\x02\u014F\u0153\x075\x02\x02\u0150\u0153\x07" +
		"\x1F\x02\x02\u0151\u0153\x077\x02\x02\u0152\u014C\x03\x02\x02\x02\u0152" +
		"\u014E\x03\x02\x02\x02\u0152\u0150\x03\x02\x02\x02\u0152\u0151\x03\x02" +
		"\x02\x02\u0153\x1F\x03\x02\x02\x02\u0154\u0155\x05\\/\x02\u0155!\x03\x02" +
		"\x02\x02\u0156\u0158\x05\\/\x02\u0157\u0159\x07\x1C\x02\x02\u0158\u0157" +
		"\x03\x02\x02\x02\u0158\u0159\x03\x02\x02\x02\u0159\u015A\x03\x02\x02\x02" +
		"\u015A\u015B\x07+\x02\x02\u015B\u015C\x07\v\x02\x02\u015C\u0161\x05\\" +
		"/\x02\u015D\u015E\x07\x06\x02\x02\u015E\u0160\x05\\/\x02\u015F\u015D\x03" +
		"\x02\x02\x02\u0160\u0163\x03\x02\x02\x02\u0161\u015F\x03\x02\x02\x02\u0161" +
		"\u0162\x03\x02\x02\x02\u0162\u0164\x03\x02\x02\x02\u0163\u0161\x03\x02" +
		"\x02\x02\u0164\u0165\x07\x04\x02\x02\u0165#\x03\x02\x02\x02\u0166\u0167" +
		"\x05\\/\x02\u0167\u0168\x07!\x02\x02\u0168\u0169\x05\\/\x02\u0169\u016A" +
		"\x07\x1F\x02\x02\u016A\u016B\x05\\/\x02\u016B%\x03\x02\x02\x02\u016C\u016D" +
		"\x05(\x15\x02\u016D\u017C\x07\v\x02\x02\u016E\u0170\x05*\x16\x02\u016F" +
		"\u0171\x05H%\x02\u0170\u016F\x03\x02\x02\x02\u0170\u0171\x03\x02\x02\x02" +
		"\u0171\u0179\x03\x02\x02\x02\u0172\u0173\x07\x06\x02\x02\u0173\u0175\x05" +
		"*\x16\x02\u0174\u0176\x05H%\x02\u0175\u0174\x03\x02\x02\x02\u0175\u0176" +
		"\x03\x02\x02\x02\u0176\u0178\x03\x02\x02\x02\u0177\u0172\x03\x02\x02\x02" +
		"\u0178\u017B\x03\x02\x02\x02\u0179\u0177\x03\x02\x02\x02\u0179\u017A\x03" +
		"\x02\x02\x02\u017A\u017D\x03\x02\x02\x02\u017B\u0179\x03\x02\x02\x02\u017C" +
		"\u016E\x03\x02\x02\x02\u017C\u017D\x03\x02\x02\x02\u017D\u017E\x03\x02" +
		"\x02\x02\u017E\u017F\x07\x04\x02\x02\u017F\'\x03\x02\x02\x02\u0180\u0181" +
		"\t\x02\x02\x02\u0181)\x03\x02\x02\x02\u0182\u0183\b\x16\x01\x02\u0183" +
		"\u0184\x07\v\x02\x02\u0184\u0185\x05*\x16\x02\u0185\u0186\x07\x04\x02" +
		"\x02\u0186\u0194\x03\x02\x02\x02\u0187\u0188\x05V,\x02\u0188\u0189\x07" +
		"!\x02\x02\u0189\u018A\x05*\x16\x0E\u018A\u0194\x03\x02\x02\x02\u018B\u0194" +
		"\x05\"\x12\x02\u018C\u0194\x05$\x13\x02\u018D\u0194\x05&\x14\x02\u018E" +
		"\u0194\x05n8\x02\u018F\u0194\x05d3\x02\u0190\u0194\x05V,\x02\u0191\u0194" +
		"\x05l7\x02\u0192\u0194\x07L\x02\x02\u0193\u0182\x03\x02\x02\x02\u0193" +
		"\u0187\x03\x02\x02\x02\u0193\u018B\x03\x02\x02\x02\u0193\u018C\x03\x02" +
		"\x02\x02\u0193\u018D\x03\x02\x02\x02\u0193\u018E\x03\x02\x02\x02\u0193" +
		"\u018F\x03\x02\x02\x02\u0193\u0190\x03\x02\x02\x02\u0193\u0191\x03\x02" +
		"\x02\x02\u0193\u0192\x03\x02\x02\x02\u0194\u01A3\x03\x02\x02\x02\u0195" +
		"\u0196\f\x0F\x02\x02\u0196\u0197\x07\b\x02\x02\u0197\u01A2\x05*\x16\x10" +
		"\u0198\u0199\f\r\x02\x02\u0199\u019A\x07\x1F\x02\x02\u019A\u01A2\x05*" +
		"\x16\x0E\u019B\u019C\f\f\x02\x02\u019C\u019D\x077\x02\x02\u019D\u01A2" +
		"\x05*\x16\r\u019E\u019F\f\v\x02\x02\u019F\u01A0\x07\t\x02\x02\u01A0\u01A2" +
		"\x05*\x16\f\u01A1\u0195\x03\x02\x02\x02\u01A1\u0198\x03\x02\x02\x02\u01A1" +
		"\u019B\x03\x02\x02\x02\u01A1\u019E\x03\x02\x02\x02\u01A2\u01A5\x03\x02" +
		"\x02\x02\u01A3\u01A1\x03\x02\x02\x02\u01A3\u01A4\x03\x02\x02\x02\u01A4" +
		"+\x03\x02\x02\x02\u01A5\u01A3\x03\x02\x02\x02\u01A6\u01A7\x070\x02\x02" +
		"\u01A7-\x03\x02\x02\x02\u01A8\u01A9\x05,\x17\x02\u01A9\u01AE\x050\x19" +
		"\x02\u01AA\u01AB\x07\x06\x02\x02\u01AB\u01AD\x050\x19\x02\u01AC\u01AA" +
		"\x03\x02\x02\x02\u01AD\u01B0\x03\x02\x02\x02\u01AE\u01AC\x03\x02\x02\x02" +
		"\u01AE\u01AF\x03\x02\x02\x02\u01AF/\x03\x02\x02\x02\u01B0\u01AE\x03\x02" +
		"\x02\x02\u01B1\u01B2\x05V,\x02\u01B2\u01B3\x05\x10\t\x02\u01B31\x03\x02" +
		"\x02\x02\u01B4\u01B5\x078\x02\x02\u01B53\x03\x02\x02\x02\u01B6\u01B7\x05" +
		"2\x1A\x02\u01B7\u01BC\x056\x1C\x02\u01B8\u01B9\x07\x06\x02\x02\u01B9\u01BB" +
		"\x056\x1C\x02\u01BA\u01B8\x03\x02\x02\x02\u01BB\u01BE\x03\x02\x02\x02" +
		"\u01BC\u01BA\x03\x02\x02\x02\u01BC\u01BD\x03\x02\x02\x02\u01BD5\x03\x02" +
		"\x02\x02\u01BE\u01BC\x03\x02\x02\x02\u01BF\u01C1\x05\\/\x02\u01C0\u01C2" +
		"\x058\x1D\x02\u01C1\u01C0\x03\x02\x02\x02\u01C1\u01C2\x03\x02\x02\x02" +
		"\u01C2\u01C4\x03\x02\x02\x02\u01C3\u01C5\x05<\x1F\x02\u01C4\u01C3\x03" +
		"\x02\x02\x02\u01C4\u01C5\x03\x02\x02\x02\u01C57\x03\x02\x02\x02\u01C6" +
		"\u01C7\x07 \x02\x02\u01C7\u01C8\x05:\x1E\x02\u01C89\x03\x02\x02\x02\u01C9" +
		"\u01CA\t\x03\x02\x02\u01CA;\x03\x02\x02\x02\u01CB\u01CC\x07<\x02\x02\u01CC" +
		"=\x03\x02\x02\x02\u01CD\u01CE\x07:\x02\x02\u01CE?\x03\x02\x02\x02\u01CF" +
		"\u01D0\x05> \x02\u01D0\u01D1\x07\"\x02\x02\u01D1\u01D3\x07\x19\x02\x02" +
		"\u01D2\u01D4\x05T+\x02\u01D3\u01D2\x03\x02\x02\x02\u01D3\u01D4\x03\x02";
	private static readonly _serializedATNSegment1: string =
		"\x02\x02\u01D4\u01F0\x03\x02\x02\x02\u01D5\u01D7\x05> \x02\u01D6\u01D8" +
		"\x07\"\x02\x02\u01D7\u01D6\x03\x02\x02\x02\u01D7\u01D8\x03\x02\x02\x02" +
		"\u01D8\u01D9\x03\x02\x02\x02\u01D9\u01DE\x05B\"\x02\u01DA\u01DB\x07\x06" +
		"\x02\x02\u01DB\u01DD\x05B\"\x02\u01DC\u01DA\x03\x02\x02\x02\u01DD\u01E0" +
		"\x03\x02\x02\x02\u01DE\u01DC\x03\x02\x02\x02\u01DE\u01DF\x03\x02\x02\x02" +
		"\u01DF\u01E2\x03\x02\x02\x02\u01E0\u01DE\x03\x02\x02\x02\u01E1\u01E3\x05" +
		"T+\x02\u01E2\u01E1\x03\x02\x02\x02\u01E2\u01E3\x03\x02\x02\x02\u01E3\u01F0" +
		"\x03\x02\x02\x02\u01E4\u01E8\x07;\x02\x02\u01E5\u01E7\x05F$\x02\u01E6" +
		"\u01E5\x03\x02\x02\x02\u01E7\u01EA\x03\x02\x02\x02\u01E8\u01E6\x03\x02" +
		"\x02\x02\u01E8\u01E9\x03\x02\x02\x02\u01E9\u01EB\x03\x02\x02\x02\u01EA" +
		"\u01E8\x03\x02\x02\x02\u01EB\u01ED\x07x\x02\x02\u01EC\u01EE\x05T+\x02" +
		"\u01ED\u01EC\x03\x02\x02\x02\u01ED\u01EE\x03\x02\x02\x02\u01EE\u01F0\x03" +
		"\x02\x02\x02\u01EF\u01CF\x03\x02\x02\x02\u01EF\u01D5\x03\x02\x02\x02\u01EF" +
		"\u01E4\x03\x02\x02\x02\u01F0A\x03\x02\x02\x02\u01F1\u01F5\x05\\/\x02\u01F2" +
		"\u01F5\x05&\x14\x02\u01F3\u01F5\x05r:\x02\u01F4\u01F1\x03\x02\x02\x02" +
		"\u01F4\u01F2\x03\x02\x02\x02\u01F4\u01F3\x03\x02\x02\x02\u01F5\u01F7\x03" +
		"\x02\x02\x02\u01F6\u01F8\x05H%\x02\u01F7\u01F6\x03\x02\x02\x02\u01F7\u01F8" +
		"\x03\x02\x02\x02\u01F8C\x03\x02\x02\x02\u01F9\u01FA\x07J\x02\x02\u01FA" +
		"\u01FB\x07z\x02\x02\u01FB\u01FD\x07{\x02\x02\u01FC\u01FE\x07z\x02\x02" +
		"\u01FD\u01FC\x03\x02\x02\x02\u01FD\u01FE\x03\x02\x02\x02\u01FE\u0203\x03" +
		"\x02\x02\x02\u01FF\u0200\x07~\x02\x02\u0200\u0202\x07z\x02\x02\u0201\u01FF" +
		"\x03\x02\x02\x02\u0202\u0205\x03\x02\x02\x02\u0203\u0201\x03\x02\x02\x02" +
		"\u0203\u0204\x03\x02\x02\x02\u0204\u0206\x03\x02\x02\x02\u0205\u0203\x03" +
		"\x02\x02\x02\u0206\u0207\x07|\x02\x02\u0207\u020B\x07}\x02\x02\u0208\u020A" +
		"\x05F$\x02\u0209\u0208\x03\x02\x02\x02\u020A\u020D\x03\x02\x02\x02\u020B" +
		"\u0209\x03\x02\x02\x02\u020B\u020C\x03\x02\x02\x02\u020C\u020E\x03\x02" +
		"\x02\x02\u020D\u020B\x03\x02\x02\x02\u020E\u020F\x07x\x02\x02\u020FE\x03" +
		"\x02\x02\x02\u0210\u0214\x07w\x02\x02\u0211\u0213\x05F$\x02\u0212\u0211" +
		"\x03\x02\x02\x02\u0213\u0216\x03\x02\x02\x02\u0214\u0212\x03\x02\x02\x02" +
		"\u0214\u0215\x03\x02\x02\x02\u0215\u0217\x03\x02\x02\x02\u0216\u0214\x03" +
		"\x02\x02\x02\u0217\u0218\x07x\x02\x02\u0218G\x03\x02\x02\x02\u0219\u021A" +
		"\x07 \x02\x02\u021A\u021C\x05J&\x02\u021B\u021D\x05N(\x02\u021C\u021B" +
		"\x03\x02\x02\x02\u021C\u021D\x03\x02\x02\x02\u021DI\x03\x02\x02\x02\u021E" +
		"\u0222\x07O\x02\x02\u021F\u0222\x05h5\x02\u0220\u0222\x05\xB8]\x02\u0221" +
		"\u021E\x03\x02\x02\x02\u0221\u021F\x03\x02\x02\x02\u0221\u0220\x03\x02" +
		"\x02\x02\u0222K\x03\x02\x02\x02\u0223\u0224\x073\x02\x02\u0224\u0232\x07" +
		"\x07\x02\x02\u0225\u0228\x07O\x02\x02\u0226\u0228\x05\xB8]\x02\u0227\u0225" +
		"\x03\x02\x02\x02\u0227\u0226\x03\x02\x02\x02\u0228\u022A\x03\x02\x02\x02" +
		"\u0229\u022B\x05N(\x02\u022A\u0229\x03\x02\x02\x02\u022A\u022B\x03\x02" +
		"\x02\x02\u022B\u022C\x03\x02\x02\x02\u022C\u022E\x07\x07\x02\x02\u022D" +
		"\u0227\x03\x02\x02\x02\u022E\u022F\x03\x02\x02\x02\u022F\u022D\x03\x02" +
		"\x02\x02\u022F\u0230\x03\x02\x02\x02\u0230\u0232\x03\x02\x02\x02\u0231" +
		"\u0223\x03\x02\x02\x02\u0231\u022D\x03\x02\x02\x02\u0232M\x03\x02\x02" +
		"\x02\u0233\u0234\x07\f\x02\x02\u0234\u0235\x07\x05\x02\x02\u0235O\x03" +
		"\x02\x02\x02\u0236\u0237\x07,\x02\x02\u0237Q\x03\x02\x02\x02\u0238\u023B" +
		"\x05P)\x02\u0239\u023C\x05t;\x02\u023A\u023C\x05\\/\x02\u023B\u0239\x03" +
		"\x02\x02\x02\u023B\u023A\x03\x02\x02\x02\u023C\u0244\x03\x02\x02\x02\u023D" +
		"\u0240\x07\x06\x02\x02\u023E\u0241\x05\\/\x02\u023F\u0241\x05t;\x02\u0240" +
		"\u023E\x03\x02\x02\x02\u0240\u023F\x03\x02\x02\x02\u0241\u0243\x03\x02" +
		"\x02\x02\u0242\u023D\x03\x02\x02\x02\u0243\u0246\x03\x02\x02\x02\u0244" +
		"\u0242\x03\x02\x02\x02\u0244\u0245\x03\x02\x02\x02\u0245S\x03\x02\x02" +
		"\x02\u0246\u0244\x03\x02\x02\x02\u0247\u0248\x07E\x02\x02\u0248\u024B" +
		"\x05V,\x02\u0249\u024A\t\x04\x02\x02\u024A\u024C\x05V,\x02\u024B\u0249" +
		"\x03\x02\x02\x02\u024B\u024C\x03\x02\x02\x02\u024C\u024F\x03\x02\x02\x02" +
		"\u024D\u024E\x07H\x02\x02\u024E\u0250\x05V,\x02\u024F\u024D\x03\x02\x02" +
		"\x02\u024F\u0250\x03\x02\x02\x02\u0250\u0254\x03\x02\x02\x02\u0251\u0252" +
		"\x07H\x02\x02\u0252\u0254\x05V,\x02\u0253\u0247\x03\x02\x02\x02\u0253" +
		"\u0251\x03\x02\x02\x02\u0254U\x03\x02\x02\x02\u0255\u0256\x05X-\x02\u0256" +
		"\u0257\x07\x07\x02\x02\u0257\u0258\x05V,\x02\u0258\u025B\x03\x02\x02\x02" +
		"\u0259\u025B\x05X-\x02\u025A\u0255\x03\x02\x02\x02\u025A\u0259\x03\x02" +
		"\x02\x02\u025BW\x03\x02\x02\x02\u025C\u025F\x05^0\x02\u025D\u025F\x05" +
		"Z.\x02\u025E\u025C\x03\x02\x02\x02\u025E\u025D\x03\x02\x02\x02\u025FY" +
		"\x03\x02\x02\x02\u0260\u0269\x07L\x02\x02\u0261\u0269\x07O\x02\x02\u0262" +
		"\u0269\x05n8\x02\u0263\u0269\x05\xB8]\x02\u0264\u0265\x07*\x02\x02\u0265" +
		"\u0266\x07\v\x02\x02\u0266\u0269\x07\x04\x02\x02\u0267\u0269\x05l7\x02" +
		"\u0268\u0260\x03\x02\x02\x02\u0268\u0261\x03\x02\x02\x02\u0268\u0262\x03" +
		"\x02\x02\x02\u0268\u0263\x03\x02\x02\x02\u0268\u0264\x03\x02\x02\x02\u0268" +
		"\u0267\x03\x02\x02\x02\u0269\u026B\x03\x02\x02\x02\u026A\u026C\x05N(\x02" +
		"\u026B\u026A\x03\x02\x02\x02\u026B\u026C\x03\x02\x02\x02\u026C[\x03\x02" +
		"\x02\x02\u026D\u026F\x07\x15\x02\x02\u026E\u026D\x03\x02\x02\x02\u026E" +
		"\u026F\x03\x02\x02\x02\u026F\u0272\x03\x02\x02\x02\u0270\u0273\x05d3\x02" +
		"\u0271\u0273\x05V,\x02\u0272\u0270\x03\x02\x02\x02\u0272\u0271\x03\x02" +
		"\x02\x02\u0273]\x03\x02\x02\x02\u0274\u0275\x07\x15\x02\x02\u0275\u0276" +
		"\x07O\x02\x02\u0276_\x03\x02\x02\x02\u0277\u0279\x05b2\x02\u0278\u027A" +
		"\x05H%\x02\u0279\u0278\x03\x02\x02\x02\u0279\u027A\x03\x02\x02\x02\u027A" +
		"a\x03\x02\x02\x02\u027B\u027E\x05V,\x02\u027C\u027E\x05d3\x02\u027D\u027B" +
		"\x03\x02\x02\x02\u027D\u027C\x03\x02\x02\x02\u027Ec\x03\x02\x02\x02\u027F" +
		"\u0280\x05V,\x02\u0280\u0282\x07\v\x02\x02\u0281\u0283\x05f4\x02\u0282" +
		"\u0281\x03\x02\x02\x02\u0282\u0283\x03\x02\x02\x02\u0283\u0284\x03\x02" +
		"\x02\x02\u0284\u0285\x07\x04\x02\x02\u0285e\x03\x02\x02\x02\u0286\u028B" +
		"\x05\\/\x02\u0287\u0288\x07\x06\x02\x02\u0288\u028A\x05\\/\x02\u0289\u0287" +
		"\x03\x02\x02\x02\u028A\u028D\x03\x02\x02\x02\u028B\u0289\x03\x02\x02\x02" +
		"\u028B\u028C\x03\x02\x02\x02\u028Cg\x03\x02\x02\x02\u028D\u028B\x03\x02" +
		"\x02\x02\u028E\u028F\t\x05\x02\x02\u028Fi\x03\x02\x02\x02\u0290\u0291" +
		"\t\x06\x02\x02\u0291k\x03\x02\x02\x02\u0292\u0295\x05h5\x02\u0293\u0295" +
		"\x05j6\x02\u0294\u0292\x03\x02\x02\x02\u0294\u0293\x03\x02\x02\x02\u0295" +
		"m\x03\x02\x02\x02\u0296\u0299\x07\f\x02\x02\u0297\u029A\x076\x02\x02\u0298" +
		"\u029A\x05p9\x02\u0299\u0297\x03\x02\x02\x02\u0299\u0298\x03\x02\x02\x02" +
		"\u029A\u029B\x03\x02\x02\x02\u029B\u029E\x07>\x02\x02\u029C\u029F\x07" +
		"6\x02\x02\u029D\u029F\x05p9\x02\u029E\u029C\x03\x02\x02\x02\u029E\u029D" +
		"\x03\x02\x02\x02\u029F\u02A0\x03\x02\x02\x02\u02A0\u02A1\x07\x05\x02\x02" +
		"\u02A1o\x03\x02\x02\x02\u02A2\u02A3\x07O\x02\x02\u02A3\u02A4\x07\x07\x02" +
		"\x02\u02A4\u02A5\x07L\x02\x02\u02A5q\x03\x02\x02\x02\u02A6\u02A7\x07I" +
		"\x02\x02\u02A7\u02A8\x05z>\x02\u02A8\u02A9\x07U\x02\x02\u02A9s\x03\x02" +
		"\x02\x02\u02AA\u02AB\x07I\x02\x02\u02AB\u02AE\x05\x90I\x02\u02AC\u02AD" +
		"\x07Y\x02\x02\u02AD\u02AF\x05v<\x02\u02AE\u02AC\x03\x02\x02\x02\u02AE" +
		"\u02AF\x03\x02\x02\x02\u02AF\u02B2\x03\x02\x02\x02\u02B0\u02B1\x07Y\x02" +
		"\x02\u02B1\u02B3\x05v<\x02\u02B2\u02B0\x03\x02\x02\x02\u02B2\u02B3\x03" +
		"\x02\x02\x02\u02B3\u02B4\x03\x02\x02\x02\u02B4\u02B5\x07U\x02\x02\u02B5" +
		"u\x03\x02\x02\x02\u02B6\u02B9\x05\x90I\x02\u02B7\u02B9\x05x=\x02\u02B8" +
		"\u02B6\x03\x02\x02\x02\u02B8\u02B7\x03\x02\x02\x02\u02B9w\x03\x02\x02" +
		"\x02\u02BA\u02BB\t\x07\x02\x02\u02BB\u02BC\x07T\x02\x02\u02BC\u02BF\x07" +
		"n\x02\x02\u02BD\u02BE\x07Y\x02\x02\u02BE\u02C0\x07o\x02\x02\u02BF\u02BD" +
		"\x03\x02\x02\x02\u02BF\u02C0\x03\x02\x02\x02\u02C0\u02C1\x03\x02\x02\x02" +
		"\u02C1\u02C2\x07U\x02\x02\u02C2y\x03\x02\x02\x02\u02C3\u02C5\x05\x86D" +
		"\x02\u02C4\u02C6\x05\x80A\x02\u02C5\u02C4\x03\x02\x02\x02\u02C5\u02C6" +
		"\x03\x02\x02\x02\u02C6\u02C8\x03\x02\x02\x02\u02C7\u02C9\x05\x82B\x02" +
		"\u02C8\u02C7\x03\x02\x02\x02\u02C8\u02C9\x03\x02\x02\x02\u02C9\u02CB\x03" +
		"\x02\x02\x02\u02CA\u02CC\x05\x88E\x02\u02CB\u02CA\x03\x02\x02\x02\u02CB" +
		"\u02CC\x03\x02\x02\x02\u02CC\u02CE\x03\x02\x02\x02\u02CD\u02CF\x05\x98" +
		"M\x02\u02CE\u02CD\x03\x02\x02\x02\u02CE\u02CF\x03\x02\x02\x02\u02CF\u02D1" +
		"\x03\x02\x02\x02\u02D0\u02D2\x05\x9AN\x02\u02D1\u02D0\x03\x02\x02\x02" +
		"\u02D1\u02D2\x03\x02\x02\x02\u02D2\u02D4\x03\x02\x02\x02\u02D3\u02D5\x05" +
		"\x9CO\x02\u02D4\u02D3\x03\x02\x02\x02\u02D4\u02D5\x03\x02\x02\x02\u02D5" +
		"\u02D7\x03\x02\x02\x02\u02D6\u02D8\x05|?\x02\u02D7\u02D6\x03\x02\x02\x02" +
		"\u02D7\u02D8\x03\x02\x02\x02\u02D8{\x03\x02\x02\x02\u02D9\u02DA\x07m\x02" +
		"\x02\u02DA\u02DB\x07o\x02\x02\u02DB}\x03\x02\x02\x02\u02DC\u02DD\x07K" +
		"\x02\x02\u02DD\u02DE\x07q\x02\x02\u02DE\u02DF\x07T\x02\x02\u02DF\u02E4" +
		"\x07q\x02\x02\u02E0\u02E1\x07Y\x02\x02\u02E1\u02E3\x07q\x02\x02\u02E2" +
		"\u02E0\x03\x02\x02\x02\u02E3\u02E6\x03\x02\x02\x02\u02E4\u02E2\x03\x02" +
		"\x02\x02\u02E4\u02E5\x03\x02\x02\x02\u02E5\u02E7\x03\x02\x02\x02\u02E6" +
		"\u02E4\x03\x02\x02\x02\u02E7\u02E8\x07U\x02\x02\u02E8\u02E9\x07R\x02\x02" +
		"\u02E9\u02EA\x05z>\x02\u02EA\u02EB\x07S\x02\x02\u02EB\x7F\x03\x02\x02" +
		"\x02\u02EC\u02F3\x05\x8CG\x02\u02ED\u02EE\x05\x92J\x02\u02EE\u02EF\x05" +
		"\x94K\x02\u02EF\u02F3\x03\x02\x02\x02\u02F0\u02F3\x05\x92J\x02\u02F1\u02F3" +
		"\x05\x94K\x02\u02F2\u02EC\x03\x02\x02\x02\u02F2\u02ED\x03\x02\x02\x02" +
		"\u02F2\u02F0\x03\x02\x02\x02\u02F2\u02F1\x03\x02\x02\x02\u02F3\x81\x03" +
		"\x02\x02\x02\u02F4\u02F5\x07k\x02\x02\u02F5\u02F6\x05\x84C\x02\u02F6\x83" +
		"\x03\x02\x02\x02\u02F7\u02F8\x07_\x02\x02\u02F8\u02F9\x05\x90I\x02\u02F9" +
		"\x85\x03\x02\x02\x02\u02FA\u02FB\x07a\x02\x02\u02FB\u02FC\x05\x96L\x02" +
		"\u02FC\x87\x03\x02\x02\x02\u02FD\u02FE\x07b\x02\x02\u02FE\u02FF\x05\x8A" +
		"F\x02\u02FF\x89\x03\x02\x02\x02\u0300\u0301\bF\x01\x02\u0301\u0302\x07" +
		"T\x02\x02\u0302\u0303\x05\x8AF\x02\u0303\u0304\x07U\x02\x02\u0304\u030D" +
		"\x03\x02\x02\x02\u0305\u0306\x07]\x02\x02\u0306\u0308\x07`\x02\x02\u0307" +
		"\u0309\x07^\x02\x02\u0308\u0307\x03\x02\x02\x02\u0308\u0309\x03\x02\x02" +
		"\x02\u0309\u030A\x03\x02\x02\x02\u030A\u030D\x05\x8AF\x04\u030B\u030D" +
		"\x05\x90I\x02\u030C\u0300\x03\x02\x02\x02\u030C\u0305\x03\x02\x02\x02" +
		"\u030C\u030B\x03\x02\x02\x02\u030D\u0317\x03\x02\x02\x02\u030E\u030F\f" +
		"\x07\x02\x02\u030F\u0310\x07[\x02\x02\u0310\u0316\x05\x8AF\b\u0311\u0312" +
		"\f\x06\x02\x02\u0312\u0313\x05\x8EH\x02\u0313\u0314\x05\x8AF\x07\u0314" +
		"\u0316\x03\x02\x02\x02\u0315\u030E\x03\x02\x02\x02\u0315\u0311\x03\x02" +
		"\x02\x02\u0316\u0319\x03\x02\x02\x02\u0317\u0315\x03\x02\x02\x02\u0317" +
		"\u0318\x03\x02\x02\x02\u0318\x8B\x03\x02\x02\x02\u0319\u0317\x03\x02\x02" +
		"\x02\u031A\u031B\x07d\x02\x02\u031B\u031C\x05\x90I\x02\u031C\u031D\x07" +
		"`\x02\x02\u031D\u031E\x05\x90I\x02\u031E\x8D\x03\x02\x02\x02\u031F\u0320" +
		"\t\b\x02\x02\u0320\x8F\x03\x02\x02\x02\u0321\u0325\x07Z\x02\x02\u0322" +
		"\u0326\x07q\x02\x02\u0323\u0326\x07n\x02\x02\u0324\u0326\x05\xA0Q\x02" +
		"\u0325\u0322\x03\x02\x02\x02\u0325\u0323\x03\x02\x02\x02\u0325\u0324\x03" +
		"\x02\x02\x02\u0326\u0339\x03\x02\x02\x02\u0327\u032B\t\t\x02\x02\u0328" +
		"\u0329\x07V\x02\x02\u0329\u032A\x07n\x02\x02\u032A\u032C\x07W\x02\x02" +
		"\u032B\u0328\x03\x02\x02\x02\u032B\u032C\x03\x02\x02\x02\u032C\u0335\x03" +
		"\x02\x02\x02\u032D\u0331\x07X\x02\x02\u032E\u0332\x07q\x02\x02\u032F\u0332" +
		"\x07o\x02\x02\u0330\u0332\x05\xA0Q\x02\u0331\u032E\x03\x02\x02\x02\u0331" +
		"\u032F\x03\x02\x02\x02\u0331\u0330\x03\x02\x02\x02\u0332\u0334\x03\x02" +
		"\x02\x02\u0333\u032D\x03\x02\x02\x02\u0334\u0337\x03\x02\x02\x02\u0335" +
		"\u0333\x03\x02\x02\x02\u0335\u0336\x03\x02\x02\x02\u0336\u0339\x03\x02" +
		"\x02\x02\u0337\u0335\x03\x02\x02\x02\u0338\u0321\x03\x02\x02\x02\u0338" +
		"\u0327\x03\x02\x02\x02\u0339\x91\x03\x02\x02\x02\u033A\u033B\x07e\x02" +
		"\x02\u033B\u033C\x07n\x02\x02\u033C\u033D\x07h\x02\x02\u033D\x93\x03\x02" +
		"\x02\x02\u033E\u033F\x07f\x02\x02\u033F\u0340\x07n\x02\x02\u0340\u0341" +
		"\x07h\x02\x02\u0341\x95\x03\x02\x02\x02\u0342\u0346\x07q\x02\x02\u0343" +
		"\u0346\x07o\x02\x02\u0344\u0346\x05\xA0Q\x02\u0345\u0342\x03\x02\x02\x02" +
		"\u0345\u0343\x03\x02\x02\x02\u0345\u0344\x03\x02\x02\x02\u0346\u0349\x03" +
		"\x02\x02\x02\u0347\u0348\x07X\x02\x02\u0348\u034A\x05\x96L\x02\u0349\u0347" +
		"\x03\x02\x02\x02\u0349\u034A\x03\x02\x02\x02\u034A\x97\x03\x02\x02\x02" +
		"\u034B\u034C\x07c\x02\x02\u034C\u0351\x05\x96L\x02\u034D\u034E\x07Y\x02" +
		"\x02\u034E\u0350\x05\x96L\x02\u034F\u034D\x03\x02\x02\x02\u0350\u0353" +
		"\x03\x02\x02\x02\u0351\u034F\x03\x02\x02\x02\u0351\u0352\x03\x02\x02\x02" +
		"\u0352\u0355\x03\x02\x02\x02\u0353\u0351\x03\x02\x02\x02\u0354\u0356\x07" +
		"g\x02\x02\u0355\u0354\x03\x02\x02\x02\u0355\u0356\x03\x02\x02\x02\u0356" +
		"\x99\x03\x02\x02\x02\u0357\u0358\x07j\x02\x02\u0358\u035D\x05\x9EP\x02" +
		"\u0359\u035A\x07Y\x02\x02\u035A\u035C\x05\x9EP\x02\u035B\u0359\x03\x02" +
		"\x02\x02\u035C\u035F\x03\x02\x02\x02\u035D\u035B\x03\x02\x02\x02\u035D" +
		"\u035E\x03\x02\x02\x02\u035E\x9B\x03\x02\x02\x02\u035F\u035D\x03\x02\x02" +
		"\x02\u0360\u0361\x07l\x02\x02\u0361\u0362\x07n\x02\x02\u0362\x9D\x03\x02" +
		"\x02\x02\u0363\u0366\x07Q\x02\x02\u0364\u0366\x05\x90I\x02\u0365\u0363" +
		"\x03\x02\x02\x02\u0365\u0364\x03\x02\x02\x02\u0366\x9F\x03\x02\x02\x02" +
		"\u0367\u0368\t\n\x02\x02\u0368\xA1\x03\x02\x02\x02\u0369\u036D\x07s\x02" +
		"\x02\u036A\u036C\x05\xA2R\x02\u036B\u036A\x03\x02\x02\x02\u036C\u036F" +
		"\x03\x02\x02\x02\u036D\u036B\x03\x02\x02\x02\u036D\u036E\x03\x02\x02\x02" +
		"\u036E\u0370\x03\x02\x02\x02\u036F\u036D\x03\x02\x02\x02\u0370\u0371\x07" +
		"t\x02\x02\u0371\xA3\x03\x02\x02\x02\u0372\u0373\x05\xA8U\x02\u0373\u0374" +
		"\x05\xA6T\x02\u0374\xA5\x03\x02\x02\x02\u0375\u0376\bT\x01\x02\u0376\u0377" +
		"\x07\v\x02\x02\u0377\u0378\x05\xA6T\x02\u0378\u0379\x07\x04\x02\x02\u0379" +
		"\u038A\x03\x02\x02\x02\u037A\u037B\x05 \x11\x02\u037B\u037C\x07\b\x02" +
		"\x02\u037C\u037D\x05 \x11\x02\u037D\u038A\x03\x02\x02\x02\u037E\u037F" +
		"\x05 \x11\x02\u037F\u0380\x07\t\x02\x02\u0380\u0381\x05 \x11\x02\u0381" +
		"\u038A\x03\x02\x02\x02\u0382\u038A\x05d3\x02\u0383\u0384\x07?\x02\x02" +
		"\u0384\u0386\x07\x1F\x02\x02\u0385\u0387\x075\x02\x02\u0386\u0385\x03" +
		"\x02\x02\x02\u0386\u0387\x03\x02\x02\x02\u0387\u0388\x03\x02\x02\x02\u0388" +
		"\u038A\x05\x1C\x0F\x02\u0389\u0375\x03\x02\x02\x02\u0389\u037A\x03\x02" +
		"\x02\x02\u0389\u037E\x03\x02\x02\x02\u0389\u0382\x03\x02\x02\x02\u0389" +
		"\u0383\x03\x02\x02\x02\u038A\u0391\x03\x02\x02\x02\u038B\u038C\f\b\x02" +
		"\x02\u038C\u038D\x05\x1E\x10\x02\u038D\u038E\x05\xA6T\t\u038E\u0390\x03" +
		"\x02\x02\x02\u038F\u038B\x03\x02\x02\x02\u0390\u0393\x03\x02\x02\x02\u0391" +
		"\u038F\x03\x02\x02\x02\u0391\u0392\x03\x02\x02\x02\u0392\xA7\x03\x02\x02" +
		"\x02\u0393\u0391\x03\x02\x02\x02\u0394\u0395\x07G\x02\x02\u0395\xA9\x03" +
		"\x02\x02\x02\u0396\u0397\x05\\/\x02\u0397\u0398\x07\b\x02\x02\u0398\u0399" +
		"\x05\xACW\x02\u0399\xAB\x03\x02\x02\x02\u039A\u039D\x05\xB8]\x02\u039B" +
		"\u039D\x07L\x02\x02\u039C\u039A\x03\x02\x02\x02\u039C\u039B\x03\x02\x02" +
		"\x02\u039D\xAD\x03\x02\x02\x02\u039E\u039F\x05\xB0Y\x02\u039F\xAF\x03" +
		"\x02\x02\x02\u03A0\u03A1\x07\n\x02\x02\u03A1\u03A6\x05\xB2Z\x02\u03A2" +
		"\u03A3\x07\x06\x02\x02\u03A3\u03A5\x05\xB2Z\x02\u03A4\u03A2\x03\x02\x02" +
		"\x02\u03A5\u03A8\x03\x02\x02\x02\u03A6\u03A4\x03\x02\x02\x02\u03A6\u03A7" +
		"\x03\x02\x02\x02\u03A7\u03A9\x03\x02\x02\x02\u03A8\u03A6\x03\x02\x02\x02" +
		"\u03A9\u03AA\x07\x03\x02\x02\u03AA\u03AE\x03\x02\x02\x02\u03AB\u03AC\x07" +
		"\n\x02\x02\u03AC\u03AE\x07\x03\x02\x02\u03AD\u03A0\x03\x02\x02\x02\u03AD" +
		"\u03AB\x03\x02\x02\x02\u03AE\xB1\x03\x02\x02\x02\u03AF\u03B0\x07M\x02" +
		"\x02\u03B0\u03B1\x07\x0E\x02\x02\u03B1\u03B2\x05\xB6\\\x02\u03B2\xB3\x03" +
		"\x02\x02\x02\u03B3\u03B4\x07\f\x02\x02\u03B4\u03B9\x05\xB6\\\x02\u03B5" +
		"\u03B6\x07\x06\x02\x02\u03B6\u03B8\x05\xB6\\\x02\u03B7\u03B5\x03\x02\x02" +
		"\x02\u03B8\u03BB\x03\x02\x02\x02\u03B9\u03B7\x03\x02\x02\x02\u03B9\u03BA" +
		"\x03\x02\x02\x02\u03BA\u03BC\x03\x02\x02\x02\u03BB\u03B9\x03\x02\x02\x02" +
		"\u03BC\u03BD\x07\x05\x02\x02\u03BD\u03C1\x03\x02\x02\x02\u03BE\u03BF\x07" +
		"\f\x02\x02\u03BF\u03C1\x07\x05\x02\x02\u03C0\u03B3\x03\x02\x02\x02\u03C0" +
		"\u03BE\x03\x02\x02\x02\u03C1\xB5\x03\x02\x02\x02\u03C2\u03CA\x07M\x02" +
		"\x02\u03C3\u03CA\x07L\x02\x02\u03C4\u03CA\x05\xB0Y\x02\u03C5\u03CA\x05" +
		"\xB4[\x02\u03C6\u03CA\x07?\x02\x02\u03C7\u03CA\x07&\x02\x02\u03C8\u03CA" +
		"\x076\x02\x02\u03C9\u03C2\x03\x02\x02\x02\u03C9\u03C3\x03\x02\x02\x02" +
		"\u03C9\u03C4\x03\x02\x02\x02\u03C9\u03C5\x03\x02\x02\x02\u03C9\u03C6\x03" +
		"\x02\x02\x02\u03C9\u03C7\x03\x02\x02\x02\u03C9\u03C8\x03\x02\x02\x02\u03CA" +
		"\xB7\x03\x02\x02\x02\u03CB\u03CC\t\v\x02\x02\u03CC\xB9\x03\x02\x02\x02" +
		"u\xBD\xC3\xC8\xCB\xCE\xD1\xD4\xD7\xDA\xDD\xE0\xE3\xE9\xF0\xFC\u0103\u0105" +
		"\u010A\u010F\u0112\u0116\u0120\u013E\u0141\u0149\u0152\u0158\u0161\u0170" +
		"\u0175\u0179\u017C\u0193\u01A1\u01A3\u01AE\u01BC\u01C1\u01C4\u01D3\u01D7" +
		"\u01DE\u01E2\u01E8\u01ED\u01EF\u01F4\u01F7\u01FD\u0203\u020B\u0214\u021C" +
		"\u0221\u0227\u022A\u022F\u0231\u023B\u0240\u0244\u024B\u024F\u0253\u025A" +
		"\u025E\u0268\u026B\u026E\u0272\u0279\u027D\u0282\u028B\u0294\u0299\u029E" +
		"\u02AE\u02B2\u02B8\u02BF\u02C5\u02C8\u02CB\u02CE\u02D1\u02D4\u02D7\u02E4" +
		"\u02F2\u0308\u030C\u0315\u0317\u0325\u032B\u0331\u0335\u0338\u0345\u0349" +
		"\u0351\u0355\u035D\u0365\u036D\u0386\u0389\u0391\u039C\u03A6\u03AD\u03B9" +
		"\u03C0\u03C9";
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
	public literal(): LiteralContext {
		return this.getRuleContext(0, LiteralContext);
	}
	public orderBySorting(): OrderBySortingContext | undefined {
		return this.tryGetRuleContext(0, OrderBySortingContext);
	}
	public orderByOrder(): OrderByOrderContext | undefined {
		return this.tryGetRuleContext(0, OrderByOrderContext);
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


