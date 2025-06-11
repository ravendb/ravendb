/// <reference path="../../typings/tsd.d.ts" />

import genUtils = require("common/generalUtils");
import moment = require("moment");
import parser = require("../../languageService/src/parser");
import antlr4ts = require("antlr4ts");
import BaseRqlParser = require("../../languageService/src/generated/BaseRqlParser");
import quoteUtils = require("../../languageService/src/quoteUtils");

class queryUtil {

    static readonly AutoPrefix = "auto/";
    static readonly DynamicPrefix = "collection/";
    static readonly AllDocs = "AllDocs";
    static readonly MinDateUTC = "0001-01-01T00:00:00.000Z"; // TODO - replace w/ syntax from RavenDB-17618 when done
    static readonly MaxDateUTC = "9999-01-01T00:00:00.000Z";

    static formatRawTimeSeriesQuery(collectionName: string, documentId: string, timeSeriesName: string, startDate?: moment.Moment, endDate?: moment.Moment) {
        const escapedCollectionName = queryUtil.escapeName(collectionName || "@all_docs");
        const escapedDocumentId = queryUtil.escapeName(documentId);
        const escapedTimeSeriesName = queryUtil.escapeName(timeSeriesName);
        const dates = queryUtil.formatDates(startDate, endDate);
        
        return `from ${escapedCollectionName}\r\nwhere id() == ${escapedDocumentId}\r\nselect timeseries(from ${escapedTimeSeriesName}${dates})`;
    }

    static formatGroupedTimeSeriesQuery(collectionName: string, documentId: string, timeSeriesName: string, group: string, startDate?: moment.Moment, endDate?: moment.Moment) {
        const escapedCollectionName = queryUtil.escapeName(collectionName || "@all_docs");
        const escapedDocumentId = queryUtil.escapeName(documentId);
        const escapedTimeSeriesName = queryUtil.escapeName(timeSeriesName);
        const dates = queryUtil.formatDates(startDate, endDate);

        return `from ${escapedCollectionName}\r\nwhere id() == ${escapedDocumentId}\r\nselect timeseries(from ${escapedTimeSeriesName}${dates} group by ${group} select avg())`;
    }
    
    private static formatDates(startDate?: moment.Moment, endDate?: moment.Moment): string {
        if (!startDate && !endDate) { 
            return "";
        }

        const start = startDate ? startDate.clone().utc().format(genUtils.utcFullDateFormat) : queryUtil.MinDateUTC;
        const end = endDate ? endDate.clone().utc().format(genUtils.utcFullDateFormat) : queryUtil.MaxDateUTC;
        
        return ` between "${start}" and "${end}"`;
    }

    static formatIndexQuery(indexName: string, fieldName: string, value: string, termType: IndexEntriesValueType) {
        const escapedFieldName = queryUtil.escapeName(fieldName);
        const escapedIndexName = queryUtil.escapeName(indexName);
        const escapedValueName = queryUtil.escapeName(value);

        const fromPart = `from index ${escapedIndexName}`;
        let wherePart;

        if (termType === "Vector") {
            wherePart = `where vector.search(${escapedFieldName}, embedding.Raw(${escapedValueName}))`;
        } else {
            wherePart = `where ${escapedFieldName} == ${escapedValueName}`;
        }

        if (indexName.startsWith("Auto") &&
            ((value.startsWith("{") && (queryUtil.hasUpperCase(value))) || value === "NULL_VALUE")) {
            if (termType === "Vector") {
                wherePart = `where exact(vector.search(${escapedFieldName}, embedding.Raw(${escapedValueName})))`;
            } else {
                wherePart = `where exact(${escapedFieldName} == ${escapedValueName})`;
            }
        }

        return `${fromPart} ${wherePart}`;
    }
    
    static hasUpperCase(value: string): boolean {
        return value.toLocaleLowerCase() !== value;
    }
    
    static wrapWithSingleQuotes(input: string) {
        if (input.includes("'")) {
            input = input.replace(/'/g, "''");
        }
        return "'" + input + "'";
    }

    static escapeName(name: string) {
        if (name.includes("\\")) {
            name = name.replace(/\\/g, "\\\\");
        }
        return queryUtil.wrapWithSingleQuotes(name);
    }

    static escapeIndexName(indexName: string): string {
        indexName = indexName.replace(/"/g, '\\"');
        
        if (indexName.toLocaleLowerCase().startsWith(queryUtil.AutoPrefix) && indexName.includes("'")) {
            return `"${indexName}"`;
        }

        return `'${indexName}'`;
    }

    static replaceSelectAndIncludeWithFetchAllStoredFields(query: string) {
        if (!query) {
            throw new Error("Query is required.");
        }
        
        const parsedRql = parser.parseRql(query);

        const rewriter = new antlr4ts.TokenStreamRewriter(parsedRql.tokenStream);
        
        const select = parsedRql.parseTree.selectStatement();
        if (select) {
            rewriter.replace(select.start, select.stop, "select __all_stored_fields");
            return rewriter.getText();
        }
        
        const include = parsedRql.parseTree.includeStatement();
        if (include) {
            rewriter.insertBefore(include.start, "select __all_stored_fields ");
            return rewriter.getText();
        }

        const limit = parsedRql.parseTree.limitStatement();
        if (limit) {
            rewriter.insertBefore(limit.start, "select __all_stored_fields ");
            return rewriter.getText();
        }

        // no select, include, limit - just insert at the end
        rewriter.insertAfter(parsedRql.parseTree.stop, " select __all_stored_fields");                    
        return rewriter.getText();
    }
    
    static getCollectionOrIndexName(query: string): [string, "index" | "collection" | "unknown"] {
        if (!query) {
            return [undefined, "unknown"];
        }
        
        const parsedRql = parser.parseRql(query);
        
        try {
            const fromStmt = parsedRql.parseTree.fromStatement();
            if (fromStmt instanceof BaseRqlParser.CollectionByIndexContext) {
                const indexName = fromStmt.indexName().text;
                return [quoteUtils.QuoteUtils.unquote(indexName), "index"];
            }
            if (fromStmt instanceof BaseRqlParser.CollectionByNameContext) {
                const collectionName = fromStmt.collectionName().text;
                return [quoteUtils.QuoteUtils.unquote(collectionName), "collection"];
            }
            return [undefined, "unknown"];
        } catch {
            return [undefined, "unknown"];
        }
    }
    
    static isDynamicQuery(query: string): boolean {
        if (!query) {
            return true;
        }
        const parsedRql = parser.parseRql(query);
        try {
            const fromStmt = parsedRql.parseTree.fromStatement();
            const fromIndex = fromStmt instanceof BaseRqlParser.CollectionByIndexContext;
            return !fromIndex;
        } catch {
            return true;
        }
    }
}

export = queryUtil;
