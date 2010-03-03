$(document).ready(function () {
    $.divanDB.init();
    
    var windowSize = $(window).height();
    var minBodySize = Math.floor(windowSize*.75);
    $('#body').css('min-height', minBodySize + 'px');
    
    $(window).resize(function() {
        var windowSize = $(window).height();
        var minBodySize = Math.floor(windowSize*.75);
        $('#body').css('min-height', minBodySize + 'px');
    });
    
    $('#nav a:not(.nav_active)').hover(function () {
        $(this).stop(true, true).animate({ backgroundColor: '#444751', color: '#fff' }, 500);
    }, function () {
        $(this).stop(true, true).animate({ backgroundColor: '#E8E9ED', color: '#000' }, 500);
    }).click(function() {
        $(this).stop(true, true).css('background-color', '#E8E9ED').css('color', '#000');
    });
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

DivanUI.SaveDocument = function (id, etag, json, successCallback) {
    $.divanDB.saveDocument(id, etag, json, successCallback);
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