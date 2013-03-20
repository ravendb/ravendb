//a useful extension to string
String.prototype.trim = function () {
    return this.replace(/^\s+|\s+$/g, "");
}

$(document).ready(function () {
    $.ravenDB.init();

    var windowSize = $(window).height();
    var minBodySize = Math.floor(windowSize * .75);
    $('#body').css('min-height', minBodySize + 'px');

    $.ajax({
        url: settings.server + '/build/version',
        dataType: 'json',
        cache: false,
        success: function (data) {
            $('#build_version').html('<a href="#">Build# ' + data.BuildVersion + '</a>')
                .click(function () {
                    alert('Build #' + data.BuildVersion + ', Version : ' + data.ProductVersion);
                });
        }
    });

    RavenUI.GetDocument("Raven/WarningMessages", function (doc, etag, metadata) {
        if (doc == null)
            return;
        $('#content').prepend("<div id='system_warning'/>")
        $('#system_warning').setTemplateURL($.ravenDB.getServerUrl() + '/raven/JSONTemplates/warningMsgs.html');
        $('#system_warning').processTemplate(doc);
    });

    $(window).resize(function () {
        var windowSize = $(window).height();
        var minBodySize = Math.floor(windowSize * .75);
        $('#body').css('min-height', minBodySize + 'px');
    });

    $('#nav a:not(.nav_active)').hover(function () {
        $(this).stop(true, true).animate({ backgroundColor: '#444751', color: '#fff' }, 500);
    }, function () {
        $(this).stop(true, true).animate({ backgroundColor: '#E8E9ED', color: '#000' }, 500);
    }).click(function () {
        $(this).stop(true, true).css('background-color', '#E8E9ED').css('color', '#000');
    });
});

function RavenUI() { }

//home page

RavenUI.UpdateQuickStats = function (targetSelector) {
    if (!$(targetSelector).hasTemplate()) {
    	$(targetSelector).setTemplateURL($.ravenDB.getServerUrl() + '/raven/JSONTemplates/quickStats.html');
    }

    $.ravenDB.getStatistics(function (stats) {
        $(targetSelector).processTemplate(stats);
    });
}

//global statistics

RavenUI.GetGlobalStatistics = function (targetSelector, afterTemplateRendered) {
    if (!$(targetSelector).hasTemplate()) {
        $(targetSelector).setTemplateURL($.ravenDB.getServerUrl() + '/raven/JSONTemplates/globalStats.html');
    }

    $.ravenDB.getStatistics(function (stats) {
        $(targetSelector).processTemplate(stats);
        if (afterTemplateRendered != null)
            afterTemplateRendered();
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

RavenUI.SaveDocument = function (id, etag, template, json, successCallback, errorCallback) {
    $.ravenDB.saveDocument(id, etag, template, json, successCallback,errorCallback);
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
    	$(targetSelector).setTemplateURL($.ravenDB.getServerUrl() + '/raven/JSONTemplates/indexPage.html');
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

RavenUI.QueryLinqIndex = function (linqQuery, pageNumber, pageSize, successCallback) {
    $.ravenDB.queryLinqIndex(linqQuery, pageNumber, pageSize, successCallback);
}

// View
RavenUI.ShowTemplatedDocument = function (docId, operation, elementName) {
    if ($.query.get('docId').length == 0) {
        $(elementName).html('No document id specified.');
        return;
    }
    RavenUI.GetDocument(docId, function (data, etag, metadata) {
        if (data == null) {
            $(elementName).html('The document "' + docId + '" could not be found');
            return;
        }
        var template = metadata['Raven-' + operation + '-Template'];
        if (template == null) {
            $(elementName).html('No ' + operation.toLowerCase() + ' template was specified for this document.');
            return;
        }
        $(elementName).setTemplateURL(metadata['Raven-' + operation + '-Template'], null, { filter_data: false });
        $(elementName).processTemplate(data);
    })
}
