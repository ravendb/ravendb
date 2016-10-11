import utils = require("utils");

var viewUnderTest = 'database/indexes/indexTerms';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind', () => {

        utils.mockCommand('commands/database/index/getIndexDefinitionCommand', () => getIndexDefinitionCommand());
        utils.mockCommand("commands/database/index/getIndexTermsCommand", () => getIndexTermsCommand());

        return utils.mockActiveDatabase(dbCtr => new dbCtr("default"))
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });

});

function getIndexTermsCommand(): string[] {
    return ["100", "101", "102"];
}

function getIndexDefinitionCommand(): Raven.Client.Indexing.IndexDefinition {
    return {
        "Name": "Orders/Totals",
        "IndexId": 2,
        "Type": "Map",
        "LockMode": "LockedIgnore",
        "MaxIndexOutputsPerDocument": null,
        "IndexVersion": -1,
        "IsSideBySideIndex": false,
        "IsTestIndex": false,
        "Reduce": null,
        "Maps": [
            "from order in docs.Orders\r\nselect new { order.Employee,  order.Company, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }"
        ],
        "Fields": {
            "Total": {
                "Analyzer": null,
                "Indexing": null,
                "Sort": "NumericDouble",
                "Storage": null,
                "Suggestions": null,
                "TermVector": null,
                "Spatial": null
            }
        }
    };
}