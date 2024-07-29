import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import { highlight, languages } from "prismjs";
import genUtils from "common/generalUtils";

class transformationScriptSyntax extends dialogViewModelBase {

    view = require("views/database/tasks/transformationScriptSyntax.html");
    
    etlType = ko.observable<StudioEtlType>();
    
    dialogContainer: Element;

    constructor(etlType: StudioEtlType) {
        super();
        
        this.etlType(etlType);
    }
    
    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.dialogContainer = document.getElementById("transformationScriptSyntaxDialog");
    }

    copySample(sampleTitle?: string) {
        let sampleText;
        const type = this.etlType();
        
        switch (type) {
            case "Raven":
                sampleText = transformationScriptSyntax.ravenEtlSamples.find(x => x.title === sampleTitle).text;
                break;
            case "Sql":
                sampleText = transformationScriptSyntax.sqlEtlSampleText;
                break;
            case "ElasticSearch":
                sampleText = transformationScriptSyntax.elasticSearchEtlSampleText;
                break;
            case "Olap":
                switch (sampleTitle) {
                    case "olapEtlSamplePartition":
                        sampleText = transformationScriptSyntax.olapEtlSamplePartitionText;
                        break;
                    case "olapEtlSampleNoPartition":
                        sampleText = transformationScriptSyntax.olapEtlSampleNoPartitionText;
                        break;
                    case "olapEtlSampleKey":
                        sampleText = transformationScriptSyntax.olapEtlSampleKeyText;
                        break;
                    case "olapEtlSampleCustomPartition":
                        sampleText = transformationScriptSyntax.olapEtlSampleCustomPartitionText;
                        break;
                }
                break;
            case "Kafka":
                sampleText = transformationScriptSyntax.kafkaEtlSampleText;
                break;
            case "RabbitMQ":
                sampleText = transformationScriptSyntax.rabbitMqEtlSampleText;
                break;
            case "AzureQueueStorage":
                sampleText = transformationScriptSyntax.azureQueueStorageEtlSampleText;
                break;
            default:
                genUtils.assertUnreachable(type, "Unknown studioEtlType: " + type);
        }
        
        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.dialogContainer);
    }

    static readonly ravenEtlSamples: Array<sampleCode> = [
        {
            title: "Simple Transformation Script",
            text:
`loadToNewEmployees ({
    Name: this.LastName + " some new data..",
    Title: this.Title + " some new data.."
});`,
            html:
`<span class="token keyword">loadTo<span class="token string">NewEmployees</span></span><span class="token punctuation"> ({</span>
    Name: this.LastName <span class="token operator">+</span> " some new data..",
    Title: this.Title <span class="token operator">+</span> " some new data.."
<span class="token punctuation">})</span>;`
        },
        {
            title: "Transformation Script with custom logic for counters",
            text:
`loadToUsers ({
    Name: this.Name + " some new data.."
});

function loadCountersOfUsersBehavior(documentId, counterName) {
    return true;
}`,
            html:
`<span class="token keyword">loadTo<span class="token string">Users</span></span><span class="token punctuation"> ({</span>
    Name: this.Name <span class="token operator">+</span> " some new data.."
<span class="token punctuation">})</span>;

function <span class="token keyword">loadCountersOf<span class="token string">Users</span>Behavior</span>(documentId, counterName) {
    return true;
}`
        },
        {
            title: "Transformation Script with custom logic for documents deletion",
            text:
`loadToUsers ({
    Name: this.Name + " some new data.."
});

function deleteDocumentsOfUsersBehavior(documentId) {
    return false; // don't propagate document deletions
}`,
            html:
`<span class="token keyword">loadTo<span class="token string">Users</span></span><span class="token punctuation"> ({</span>
    Name: this.Name <span class="token operator">+</span> " some new data.."
<span class="token punctuation">})</span>;

function <span class="token keyword">deleteDocumentsOf<span class="token string">Users</span>Behavior</span>(documentId) {
    return false; <span class="token comment">// don't propagate document deletions</span>
}`
        }
    ];
    
    static readonly sqlEtlSampleText =
 `var orderData = {
    Id: id(this),
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    orderData.TotalCost += line.PricePerUnit;
    
    // Load to SQL table 'OrderLines'
    loadToOrderLines({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}
orderData.TotalCost = Math.round(orderData.TotalCost * 100) / 100;

// Load to SQL table 'Orders'
loadToOrders(orderData);`;

    sqlEtlSampleHtml = transformationScriptSyntax.highlightJavascript(transformationScriptSyntax.sqlEtlSampleText);

    static readonly elasticSearchEtlSampleText =
`var orderData = {
    Id: id(this), // property with RavenDB document ID
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    var cost = (line.Quantity * line.PricePerUnit) * ( 1 - line.Discount);
    orderData.TotalCost += cost;
    loadToOrderLines({
        OrderId: id(this), // property with RavenDB document ID
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}

loadToOrders(orderData); // load to Elasticsearch Index 'orders'`;

    elasticSearchEtlSampleHtml = transformationScriptSyntax.highlightJavascript(transformationScriptSyntax.elasticSearchEtlSampleText);

    static readonly queueEtlBaseSampleText =
`var orderData = {
    Id: id(this), // property with RavenDB document ID
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    var cost = (line.Quantity * line.PricePerUnit) * ( 1 - line.Discount);
    orderData.TotalCost += cost;
}`;

    static readonly kafkaEtlSampleText =
`${transformationScriptSyntax.queueEtlBaseSampleText}

loadToOrders(orderData, {  // load to the 'Orders' Topic with optional params
    Id: id(this),
    PartitionKey: id(this),
    Type: 'com.github.users',
    Source: '/registrations/direct-signup'
});`;

    static readonly rabbitMqEtlSampleText =
`${transformationScriptSyntax.queueEtlBaseSampleText}

loadToOrders(orderData, "routingKey", {  // load to the 'Orders' Exchange with optional params
    Id: id(this),
    Type: 'com.github.users',
    Source: '/registrations/direct-signup'
});`;
    
    static readonly azureQueueStorageEtlSampleText =
        `${transformationScriptSyntax.queueEtlBaseSampleText}

loadToOrders(orderData, {  // load to the 'Orders' Queue with optional params
    Id: id(this),
    Type: 'com.github.users',
    Source: '/registrations/direct-signup'
});`;
    
    kafkaEtlSampleHtml = transformationScriptSyntax.highlightJavascript(transformationScriptSyntax.kafkaEtlSampleText);
    
    rabbitMqEtlSampleHtml = transformationScriptSyntax.highlightJavascript(transformationScriptSyntax.rabbitMqEtlSampleText);
    azureQueueStorageEtlSampleHtml = transformationScriptSyntax.highlightJavascript(transformationScriptSyntax.azureQueueStorageEtlSampleText);
    
    static readonly olapEtlSamplePartitionText =
`var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth() + 1;

loadToOrders(partitionBy(['year', year], ['month', month]), {
    // The order of params in the partitionBy method determines the parquet file path
    Company: this.Company,
    ShipVia: this.ShipVia
    // Note: 2 more field are always created per table by default:
    //       * _id: The ID column - can be overriden in the task definition
    //       * _lastModifiedTime: The document's last modification time column - cannot be overriden
});`;
    
    olapEtlSamplePartitionHtml = transformationScriptSyntax.highlightJavascript(transformationScriptSyntax.olapEtlSamplePartitionText);

    static readonly olapEtlSampleNoPartitionText =
`loadToOrders(noPartition(), {
    // Data will Not be partitioned
    Company: this.Company
});`;
    
    olapEtlSampleNoPartitionHtml = transformationScriptSyntax.highlightJavascript(transformationScriptSyntax.olapEtlSampleNoPartitionText);

    static readonly olapEtlSampleKeyText = 
`var key = new Date(this.OrderedAt);
loadToOrders(partitionBy(key), {
    // The partition that will be created will be: "_partition={key}"
    Company: this.Company
});`;
    
    olapEtlSampleKeyHtml = transformationScriptSyntax.highlightJavascript(transformationScriptSyntax.olapEtlSampleKeyText);

    static readonly olapEtlSampleCustomPartitionText =
`var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();

loadToOrders(partitionBy(['year', year], ['customPartitionName', $customPartitionValue]), {
    // The 'customPartitionValue' is set in the OLAP task definition
    Company: this.Company
});`;

    olapEtlSampleCustomPartitionHtml = transformationScriptSyntax.highlightJavascript(transformationScriptSyntax.olapEtlSampleCustomPartitionText);

    static highlightJavascript(source: string) {
        return highlight(source, languages.javascript, "js");
    }
}

export = transformationScriptSyntax;
