$(document).ready(function () {
    $.divanDB.init();
});

function DivanUI() { }

//home page

DivanUI.UpdateQuickStats = function (targetSelector) {
    if (!$(targetSelector).hasTemplate()) {
        $(targetSelector).setTemplateURL('JSONTemplates/quickStats.html');
    }

    $.divanDB.getStatistics(function (stats) {
        $(targetSelector).processTemplate(stats);
    });
}

//global statistics

DivanUI.GetGlobalStatistics = function (targetSelector) {
    if (!$(targetSelector).hasTemplate()) {
        $(targetSelector).setTemplateURL('JSONTemplates/globalStats.html');
    }

    $.divanDB.getStatistics(function (stats) {
        $(targetSelector).processTemplate(stats);
    });
}

//Documents
DivanUI.GetDocumentCount = function (successCallback) {
    $.divanDB.getDocumentCount(successCallback);
}

DivanUI.GetDocumentPage = function (pageNum, pageSize, successCallback) {
    $.divanDB.getDocumentPage(pageNum, pageSize, function (docs) {
        successCallback(docs);
    });
}

DivanUI.GetDocument = function (id, successCallback) {
    $.divanDB.getDocument(id, successCallback);
}

DivanUI.SaveDocument = function (id, json, successCallback) {
    $.divanDB.saveDocument(id, json, successCallback);
}

//indexes
DivanUI.GetIndexCount = function (successCallback) {
    $.divanDB.getIndexCount(successCallback);
}

DivanUI.GetIndexPage = function (pageNum, pageSize, targetSelector, successCallback) {
    if (!$(targetSelector).hasTemplate()) {
        $(targetSelector).setTemplateURL('JSONTemplates/indexPage.html');
    }

    $.divanDB.getIndexPage(pageNum, pageSize, function (indexes) {
        $(targetSelector).processTemplate(indexes);
        successCallback();
    });
}

DivanUI.GetIndex = function (name, successCallback) {
    $.divanDB.getIndex(name, successCallback);
}

DivanUI.SaveIndex = function (name, def, successCallback) {
    $.divanDB.saveIndex(name, def, successCallback);
}

DivanUI.SearchIndexes = function (name, successCallback) {
    $.divanDB.searchIndexes(name, successCallback);
}

DivanUI.QueryIndex = function (name, queryValues, pageNumber, pageSize, successCallback) {
    $.divanDB.queryIndex(name, queryValues, pageNumber, pageSize, successCallback);
}