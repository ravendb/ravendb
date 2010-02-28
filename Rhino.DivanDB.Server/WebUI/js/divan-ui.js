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

DivanUI.GetDocumentPage = function(pageNum, pageSize, targetSelector) {
    if (!$(targetSelector).hasTemplate()) {
        $(targetSelector).setTemplateURL('JSONTemplates/documentPage.html');
    }

    $.divanDB.getDocumentPage(pageNum, pageSize, function (docs) {
        $(targetSelector).processTemplate(docs);
    });
}

//indexes
DivanUI.GetIndexCount = function (successCallback) {
    $.divanDB.getIndexCount(successCallback);
}

DivanUI.GetIndexPage = function (pageNum, pageSize, targetSelector) {
    if (!$(targetSelector).hasTemplate()) {
        $(targetSelector).setTemplateURL('JSONTemplates/indexPage.html');
    }

    $.divanDB.getIndexPage(pageNum, pageSize, function (indexes) {
        $(targetSelector).processTemplate(indexes);
    });
}