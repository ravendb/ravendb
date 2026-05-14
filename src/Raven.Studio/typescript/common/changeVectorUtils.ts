/// <reference path="../../typings/tsd.d.ts" />

import typeUtils = require("common/typeUtils");

class changeVectorUtils {

    static shouldUseLongFormat(changeVectors: string[]) {
        const parsedVectors = changeVectors.flatMap(x => changeVectorUtils.parseEntries(x));

        const byTag = _.groupBy(parsedVectors, (x: changeVectorEntryItem) => x.tag);

        return Object.values(byTag).some((forTag: changeVectorEntryItem[]) => forTag.map((x) => x.dbId).length > 1);
    }

    private static parsePart(part: string) {
        if (!part) {
            return [];
        }

        let tokens = part.split(",")
                         .map(cvEntry => changeVectorUtils.parseChangeVectorEntry(cvEntry));

        tokens = typeUtils.sortBy(tokens, x => x.tag);

        return tokens;
    }

    private static parseEntries(input: string) {
        return changeVectorUtils.parsePart(input?.replaceAll("|", ",") ?? "");
    }

    static formatChangeVector(input: string, useLongChangeVectorFormat: boolean): changeVectorItem[] {
        //A:1066-8Bk5eyIYfES1TzuU6TnzPg, C:1066-iazDDYGWiUmj8AwW4jgjYA, E:1066-m9yioKcvEkGny6tfKJo3Tw, B:1068-5hNdZ22Up0e+KkaU7u2VUg, D:1066-OHQUXCEyYU6VE
        if (!input) {
            return [];
        }

        const toItem = (x: ReturnType<typeof changeVectorUtils.parseChangeVectorEntry>): changeVectorItem => ({
            fullFormat: x.original,
            shortFormat: useLongChangeVectorFormat
                ? `${x.tag}:${x.etag}-${x.dbId.substring(0, 4)}...`
                : `${x.tag}:${x.etag}`
        });

        const pipeIndex = input.indexOf("|");
        if (pipeIndex === -1) {
            return changeVectorUtils.parsePart(input).map(toItem);
        }

        // Dual CV format: "Order|Version" — merge boundary tokens into one badge e.g. "B:18|A:12"
        const orderItems = changeVectorUtils.parsePart(input.substring(0, pipeIndex)).map(toItem);
        const versionItems = changeVectorUtils.parsePart(input.substring(pipeIndex + 1)).map(toItem);
        const bridge: changeVectorItem = {
            fullFormat: `${orderItems.at(-1).fullFormat} | ${versionItems[0].fullFormat}`,
            shortFormat: `${orderItems.at(-1).shortFormat} | ${versionItems[0].shortFormat}`
        };
        return [...orderItems.slice(0, -1), bridge, ...versionItems.slice(1)];
    }

    static formatChangeVectorAsShortString(input: string) {
        //A:1066-8Bk5eyIYfES1TzuU6TnzPg, C:1066-iazDDYGWiUmj8AwW4jgjYA, E:1066-m9yioKcvEkGny6tfKJo3Tw, B:1068-5hNdZ22Up0e+KkaU7u2VUg, D:1066-OHQUXCEyYU6VE
        const tokens = changeVectorUtils.parseEntries(input);
        return tokens.map(x => `${x.tag}:${x.etag}`).join(", ");
    }
    
    static getDatabaseID(cvEntry: string): string {
        const tokens = changeVectorUtils.parseChangeVectorEntry(cvEntry);
        return tokens.dbId;
    }
    
    private static parseChangeVectorEntry(cvEntry: string): changeVectorEntryItem {
        const trimmedValue = cvEntry.trim();

        const [tag, rest] = trimmedValue.split(":", 2);
        const [etag, dbId] = rest.split("-", 2);

        return {
            tag,
            etag,
            dbId,
            original: trimmedValue
        };
    }
}

export = changeVectorUtils;
