/// <reference path="../../typings/tsd.d.ts" />

class changeVectorUtils {

    static noDocumentFoundPossibleReasonsHtml =
        "<small><strong>Possible reasons:</strong>" +
        "<ul style='text-align:left'>" +
        "<li>A change-vector changes with every document change.<br>" +
        "The given change-vector may not be valid anymore if the document was either modified or deleted.</li>" +
        "<br>" + 
        "<li>Search is done on the current node (opened in this Studio tab).<br>" +
        "The search may not find the associated document if it was replicated/imported to this node - but wasn't modified on this node - prior to being sent by the subscription task to the client.</li>" +
        "</ul></small>";
    
    static shouldUseLongFormat(changeVectors: string[]) {
        const parsedVectors = _.flatMap(changeVectors, x => changeVectorUtils.parse(x));
        
        const byTag = _.groupBy(parsedVectors, x => x.tag);
        
        return _.some(byTag, forTag => forTag.map(x => x.dbId).length > 1);
    }
    
    private static parse(input: string) {
        if (!input) {
            return [];
        }
        let tokens = input.split(",").map(x => {
            const trimmedValue = _.trim(x);

            const [tag, rest] = trimmedValue.split(":", 2);
            const [etag, dbId] = rest.split("-", 2);

            return {
                tag: tag,
                etag: etag,
                dbId: dbId,
                original: x
            };
        });

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
} 

export = changeVectorUtils;
