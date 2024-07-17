/// <reference path="../../typings/tsd.d.ts" />

import { sortBy } from "common/typeUtils";

class changeVectorUtils {

    static shouldUseLongFormat(changeVectors: string[]) {
        const parsedVectors = changeVectors.flatMap(x => changeVectorUtils.parse(x));
        
        const byTag = _.groupBy(parsedVectors, (x: { tag: string}) => x.tag);
        
        return Object.values(byTag).some((forTag: any) => forTag.map((x: typeof parsedVectors[0]) => x.dbId).length > 1);
    }
    
    private static parse(input: string) {
        if (!input) {
            return [];
        }
        
        let tokens = input.split(",")
                          .map(cvEntry => changeVectorUtils.parseChangeVectorEntry(cvEntry));
        
        tokens = sortBy(tokens, x => x.tag);

        return tokens;
    }
    
    static formatChangeVector(input: string, useLongChangeVectorFormat: boolean): changeVectorItem[] {
        //A:1066-8Bk5eyIYfES1TzuU6TnzPg, C:1066-iazDDYGWiUmj8AwW4jgjYA, E:1066-m9yioKcvEkGny6tfKJo3Tw, B:1068-5hNdZ22Up0e+KkaU7u2VUg, D:1066-OHQUXCEyYU6VE
        const tokens = changeVectorUtils.parse(input);
       
        if (useLongChangeVectorFormat) {
            return tokens.map((x): changeVectorItem => {
                return {
                    fullFormat: x.original,
                    shortFormat: `${x.tag}:${x.etag}-${x.dbId.substring(0, 4)}...`
                };
            });
        } else {
            return tokens.map((x): changeVectorItem => {
                return {
                    fullFormat: x.original,
                    shortFormat: `${x.tag}:${x.etag}`
                };
            });
        }
    }

    static formatChangeVectorAsShortString(input: string) {
        //A:1066-8Bk5eyIYfES1TzuU6TnzPg, C:1066-iazDDYGWiUmj8AwW4jgjYA, E:1066-m9yioKcvEkGny6tfKJo3Tw, B:1068-5hNdZ22Up0e+KkaU7u2VUg, D:1066-OHQUXCEyYU6VE
        const tokens = changeVectorUtils.parse(input);
        return tokens.map(x => `${x.tag}:${x.etag}`).join(", ");
    }
    
    static getDatabaseID(cvEntry: string): string {
        const tokens = changeVectorUtils.parseChangeVectorEntry(cvEntry);
        return tokens.dbId;
    }
    
    private static parseChangeVectorEntry(cvEntry: string) {
        const trimmedValue = cvEntry.trim();

        const [tag, rest] = trimmedValue.split(":", 2);
        const [etag, dbId] = rest.split("-", 2);

        return {
            tag: tag,
            etag: etag,
            dbId: dbId,
            original: trimmedValue
        };
    }
} 

export = changeVectorUtils;
