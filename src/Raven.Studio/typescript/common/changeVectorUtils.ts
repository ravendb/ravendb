/// <reference path="../../typings/tsd.d.ts" />

class changeVectorUtils {

    static shouldUseLongFormat(changeVectors: string[]) {
        const parsedVectors = _.flatMap(changeVectors, x => changeVectorUtils.parse(x));
        
        const byTag = _.groupBy(parsedVectors, x => x.tag);
        
        return _.some(byTag, forTag => forTag.map(x => x.dbId).length > 1);
    }
    
    private static parse(input: string) {
        if (!input) {
            return [];
        }
        
        let tokens = input.split(",")
                          .map(cvEntry => changeVectorUtils.parseChangeVectorEntry(cvEntry));
        
        tokens = _.sortBy(tokens, x => x.tag);

        return tokens;
    }
    
    static formatChangeVector(input: string, useLongChangeVectorFormat: boolean): changeVectorItem[] {
        //A:1066-8Bk5eyIYfES1TzuU6TnzPg, C:1066-iazDDYGWiUmj8AwW4jgjYA, E:1066-m9yioKcvEkGny6tfKJo3Tw, B:1068-5hNdZ22Up0e+KkaU7u2VUg, D:1066-OHQUXCEyYU6VE
        const tokens = changeVectorUtils.parse(input);
       
        if (useLongChangeVectorFormat) {
            return tokens.map(x => {
                return {
                    fullFormat: x.original,
                    shortFormat: `${x.tag}:${x.etag}-${x.dbId.substring(0, 4)}...`
                } as changeVectorItem;
            });
        } else {
            return tokens.map(x => {
                return {
                    fullFormat: x.original,
                    shortFormat: `${x.tag}:${x.etag}`
                } as changeVectorItem;
            });
        }
    }

    static formatChangeVectorAsShortString(input: string) {
        //A:1066-8Bk5eyIYfES1TzuU6TnzPg, C:1066-iazDDYGWiUmj8AwW4jgjYA, E:1066-m9yioKcvEkGny6tfKJo3Tw, B:1068-5hNdZ22Up0e+KkaU7u2VUg, D:1066-OHQUXCEyYU6VE
        const tokens = changeVectorUtils.parse(input);
        return tokens.map(x => `${x.tag}:${x.etag}`).join(", ");
    }
    
    static getDatabaseID(cvEntry: string) {
        const tokens = changeVectorUtils.parseChangeVectorEntry(cvEntry);
        return tokens.dbId;
    }
    
    private static parseChangeVectorEntry(cvEntry: string) {
        const trimmedValue = _.trim(cvEntry);

        const [tag, rest] = trimmedValue.split(":", 2);
        const [etag, dbId] = rest.split("-", 2);

        return {
            tag: tag,
            etag: etag,
            dbId: dbId,
            original: cvEntry
        };
    }
} 

export = changeVectorUtils;
