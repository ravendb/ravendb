$(document).ready(function () {
    $.ravenDB.init();
    
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

function RavenUI() { }

//home page

RavenUI.UpdateQuickStats = function (targetSelector) {
    if (!$(targetSelector).hasTemplate()) {
        $(targetSelector).setTemplateURL('JSONTemplates/quickStats.html');
    }

    $.ravenDB.getStatistics(function (stats) {
        $(targetSelector).processTemplate(stats);
    });
}

//global statistics

RavenUI.GetGlobalStatistics = function (targetSelector) {
    if (!$(targetSelector).hasTemplate()) {
        $(targetSelector).setTemplateURL('JSONTemplates/globalStats.html');
    }

    $.ravenDB.getStatistics(function (stats) {
        $(targetSelector).processTemplate(stats);
    });
}

//Documents
RavenUI.GetDocumentCount = function (successCallback) {
    $.ravenDB.getDocumentCount(successCallback);
}

RavenUI.GetDocumentPage = function (pageNum, pageSize, successCallback) {
    $.ravenDB.getDocumentPage(pageNum, pageSize, function (docs) {
        successCallback(docs);
    });
}

RavenUI.GetDocument = function (id, successCallback) {
    $.ravenDB.getDocument(id, successCallback);
}

RavenUI.SaveDocument = function (id, etag, template, json, successCallback) {
    $.ravenDB.saveDocument(id, etag, template, json, successCallback);
}

RavenUI.DeleteDocument = function (id, etag, successCallback) {
    $.ravenDB.deleteDocument(id, etag, successCallback);
}

//indexes
RavenUI.GetIndexCount = function (successCallback) {
    $.ravenDB.getIndexCount(successCallback);
}

RavenUI.GetIndexPage = function (pageNum, pageSize, targetSelector, successCallback) {
    if (!$(targetSelector).hasTemplate()) {
        $(targetSelector).setTemplateURL('JSONTemplates/indexPage.html');
    }

    $.ravenDB.getIndexPage(pageNum, pageSize, function (indexes) {
        $(targetSelector).processTemplate(indexes);
        successCallback();
    });
}

RavenUI.GetIndex = function (name, successCallback) {
    $.ravenDB.getIndex(name, successCallback);
}

RavenUI.SaveIndex = function (name, def, successCallback, errorCallback) {
    $.ravenDB.saveIndex(name, def, successCallback, errorCallback);
}

RavenUI.DeleteIndex = function (name, successCallback) {
    $.ravenDB.deleteIndex(name, successCallback);
}

RavenUI.SearchIndexes = function (name, successCallback) {
    $.ravenDB.searchIndexes(name, successCallback);
}

RavenUI.QueryIndex = function (name, queryValues, pageNumber, pageSize, successCallback) {
    $.ravenDB.queryIndex(name, queryValues, pageNumber, pageSize, successCallback);
}

// View
RavenUI.ShowTemplatedDocument = function(docId, operation, elementName) {
    if ($.query.get('docId').length == 0) {
        $(elementName).html('No document id specified.');
        return;
    }
    RavenUI.GetDocument(docId, function(data, xhr) {
        var template = xhr.getResponseHeader('Raven-' + operation + '-Template');
        if (template == null) {
            $(elementName).html('No ' + operation + ' template was specified for this document.');
            return;
        }
        $(elementName).setTemplateURL(template);
        $(elementName).processTemplate(JSON.parse(data));
    })
}