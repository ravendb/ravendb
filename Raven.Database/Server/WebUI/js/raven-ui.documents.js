var numPages;
var pageNumber = 1;
var pageSize = 25;
var allDocsTotalCount;
var indexName;
var indexValues;
var linqQuery;
var queryMode = 'allDocs';
var isInQueryMode = false;

$(document).ready(function () {
    RavenUI.GetDocumentCount(function (count) {
        allDocsTotalCount = count;
        getAllDocuments();
    });

    $('#createNewDocument').button({
        icons: { primary: 'ui-icon-plusthick' }
    }).click(function () {
        CreateDocument();
    });

    $('#executeQuery').button({
        icons: { primary: 'ui-icon-circle-triangle-e' }
    }).click(function () {
        indexName = $('#txtIndexName').val();
        indexValues = $('#txtIndexValue').val();
        ExecuteQuery();
    });

    $('#executeLinqQuery').button({
        icons: { primary: 'ui-icon-circle-triangle-e' }
    }).click(function () {
        linqQuery = $('#txtLinqQuery').val();
        ExecuteLinqQuery();
    });

    $('#executeGetByDoumentId').button({
        icons: { primary: 'ui-icon-circle-triangle-e' }
    }).click(function () {
        EditDocument($('#txtDocumentId').val());
    });

    $('#txtIndexName').autocomplete({
        source: function (request, response) {
            RavenUI.SearchIndexes(request.term, function (searchResult) {
                response(searchResult);
            });
        },
        minLength: 1,
        select: function (event, ui) {
            $('#queryValueSubmit').fadeIn();
        }
    }).keyup(function (event) {
        if (event.keyCode != 13) {
            $('#queryValueSubmit').fadeOut();
        }
    });
});

function getAllDocuments() {
    $('#ajaxError').slideUp();
     $('#docList').show().html('<img src="images/ajax-loader.gif" /> Loading...');
    RavenUI.GetDocumentPage(pageNumber, pageSize, function (docs) {
        if (docs.length == 0) {
            $('#docList').html('There are no documents in your database.');
            $('#pager').hide();
        } else {
            processDocumentResults(docs, allDocsTotalCount);
        }
    });
}

function ExecuteLinqQuery() {
    queryMode = 'linearQuery';
    $('#ajaxError').slideUp();
    $('#docList').show().html('<img src="images/ajax-loader.gif" /> Loading...');

    RavenUI.QueryLinqIndex(linqQuery, pageNumber, pageSize, function (data) {
        if (data.Results.length == 0) {
            $('#docList').html('No documents matched your query.');
            $('#pager').hide();
        } else {
            //this is only here because there's no way to get the total count on a query currently
            allDocsTotalCount = data.TotalResults;
            if (data.Errors.length > 0) {
                $('#ajaxError').setTemplateURL($.ravenDB.getServerUrl() + '/raven/JSONTemplates/errorsMsgs.html');
                $('#ajaxError').processTemplate(data);
                $('#ajaxError').slideDown();
            }
            processDocumentResults(data.Results, allDocsTotalCount);
        }
    });
}

function ExecuteQuery() {
    $('#ajaxError').slideUp();
     queryMode = 'indexQuery';

    $('#docList').show().html('<img src="images/ajax-loader.gif" /> Loading...');

    RavenUI.QueryIndex(indexName, indexValues, pageNumber, pageSize, function (data) {
        if (data.Results.length == 0) {
            $('#docList').html('No documents matched your query.');
            $('#pager').hide();
        } else {
            //this is only here because there's no way to get the total count on a query currently
            allDocsTotalCount = data.Results.length;
            processDocumentResults(data.Results, allDocsTotalCount);
        }
    });
}

function getDisplayString(docID, json, metadata) {
	var entityName = "";
	var returnJSON = json;
	if (metadata != null && metadata["Raven-Entity-Name"] != null) {
		entityName = " (" + metadata["Raven-Entity-Name"] + ")";
	}
    delete returnJSON["@metadata"];
    var jsonString = JSON.stringify(returnJSON)
				.replace(/</g, '&lt;')
				.replace(/>/g, '&gt;');
    if (jsonString.length > 90)
    	jsonString = jsonString.substring(0, 90) + '...';
    return "<span style='float:right'><b>" + docID + entityName + "</b></span>" + jsonString;
}

function processDocumentResults(results, totalCount) {
    numPages = Math.ceil(totalCount / pageSize);
    $("#pager").pager({
        pagenumber: pageNumber,
        pagecount: numPages,
        buttonClickCallback: pagerClick
    });

    var wrapper = $('<div class="searchListWrapper ui-corner-all"></div>');
    var alternate = false;
    $(results).each(function () {
    	var docID = "Projection";
    	var metadata = this['@metadata'];
    	if (metadata != null && metadata['@id'] != null)
    		docID = metadata['@id'];
    	var previewHTML = GetDocumentViewHTML(this);
    	if (alternate) {
    		var searchResult = $('<div id="' + docID + '" class="searchListItem alternate"></div>');
    	} else {
    		var searchResult = $('<div id="' + docID + '" class="searchListItem"></div>');
    	}
    	var doc = this;
    	alternate = !alternate;
    	$(searchResult).html(getDisplayString(docID, doc, metadata));
    	$(searchResult).click(function () {
    		if (docID != "Projection") {
    			EditDocument(docID);
    			return;
    		}
    		ShowEditorForDocument(docID, doc, null, metadata, 'Show Document Projection', function (id, etag, metadata, json, editor) {
    			$('.dialogError', editor).html('You cannot modify a document projection').slideDown();
    		});
    	});

    	var previousBGColor;
    	$(searchResult).hover(function () {
    		previousBGColor = $(this).css('background-color');
    		$(this).css('background-color', '#94C2D8');
    		var x = $(this).position().left + $(this).outerWidth();
    		var y = $('.searchListWrapper').position().top;
    		var width = $('#body').width() - x - 20;

    		var jsonPreview = $('<div class="jsonViewWrapper"></div>');
    		$(jsonPreview).html(previewHTML);
    		$(jsonPreview).find('.jsonObjectView:first').css('border', 'none').css('padding', '0').css('margin-left', '0');
    		$(jsonPreview).prepend("<h2>Click to edit this document</h2><h3>Document Preview:</h3>");
    		$('#documentPreview')
                        .css('width', width + 'px')
                        .css('position', 'absolute')
                        .css('left', x + 'px')
                        .css('top', y + 'px')
                        .html(jsonPreview);
    		if ($('#documentPreview').is(":animated"))
    			$('#documentPreview').stop().show().fadeTo("normal", 0.9);
    		else
    			$('#documentPreview').is(':visible') ? $('#documentPreview').fadeTo("normal", 0.9) : $('#documentPreview').fadeTo("normal", 0.9);
    	}, function () {
    		$(this).css('background-color', previousBGColor);
    		if ($('#documentPreview').is(':animated'))
    			$('#documentPreview').stop().fadeTo("normal", 0, function () { $(this).hide() });
    		else
    			$('#documentPreview').stop().fadeOut();
    	});
    	$(wrapper).append(searchResult);
    });

    $('#docList').html(wrapper);
}

function pagerClick(newPageNumber) {
    pageNumber = newPageNumber;
    switch (queryMode) {
        case 'allDocs':
            getAllDocuments();
            break;
        case 'indexQuery':
            ExecuteQuery();
            break;
        case 'linearQuery':
            ExecuteLinqQuery();
            break;
    }
}

function EditDocument(id) {
    $('#ajaxSuccess, #ajaxError').fadeOut();
    RavenUI.GetDocument(id, function (doc, etag, metadata) {

        if (doc == null) {
            $('#ajaxError').html('Document with id ' + id + ' could not be found').slideDown().delay(1400).slideUp();
            return;
        }

        ShowEditorForDocument(id, doc, etag, metadata, 'Edit Document', function (id, etag, metadata, json, editor) {
            RavenUI.SaveDocument(id, etag, metadata, json, function () {
                $(editor).dialog('close');
                $('#ajaxSuccess').html('Your document has been updated. Click <a href="#" onclick="EditDocument(\'' + id + '\'); return false;">here</a> to see it again.').fadeIn('slow');
                if (!isInQueryMode) {
                    getAllDocuments();
                } else {
                    ExecuteQuery();
                }
            }, function (errorMsg) {
                $('.dialogError', editor).html('Your document could not be saved because: ' + errorMsg).slideDown();
            });
        }, function (id, etag, editor, deleteDialog) {
            RavenUI.DeleteDocument(id, etag, function (data) {
                $(deleteDialog).dialog('close');
                $(editor).dialog('close');
                $('#ajaxSuccess').html('Your document has been deleted.').fadeIn('slow');
                if (!isInQueryMode) {
                    getAllDocuments();
                } else {
                    ExecuteQuery();
                }
            });
        });
    });
}

function CreateDocument() {
    $('#ajaxSuccess, #ajaxError').fadeOut();
    ShowEditorForNewDocument(function (id, metadata, json, editor) {
        RavenUI.SaveDocument(id, null, metadata, json, function (data) {
            $(editor).dialog('close');
            $('#ajaxSuccess').html('Your document has been created. Click <a href="#" onclick="EditDocument(\'' + data.Key + '\'); return false;">here</a> to see it again.').fadeIn('slow');
            if (!isInQueryMode) {
                getAllDocuments();
            } else {
                ExecuteQuery();
            }
        }, function (errorMsg) {
            $('.dialogError', editor).html('Your document could not be created because: ' + errorMsg).slideDown();
        });
    });
}

function GetDocumentViewHTML(json, template) {

    var view = $('<div class="jsonViewWrapper"></div>');
    if (json["@metadata"])
        delete json["@metadata"];
    $(view).html(GetHTMLForDefaultView(json));

    $(view).find('.jsonObjectView:first').css('border', 'none').css('padding', '0').css('margin-left', '0');

    $(view).find('.arrayNameView').click(function () {
        $(this).next().slideToggle();
        var current = $(this).children('div').html();
        if (current == 'hide')
            $(this).children('div').html('show');
        else
            $(this).children('div').html('hide');
    }).hover(function () {
        var text;
        if ($(this).next().is(':visible')) {
            text = 'hide';
        } else {
            text = 'show';
        }
        $(this).append('<div style="position:absolute; right: 5px; top: 8px; font-size: 10px; color: #ccc;">' + text + '</div>');
    }, function () {
        $(this).children('div').remove();
    });

    return view;
}

function GetHTMLForDefaultView(jsonObj) {
    if (typeof jsonObj == "object") {
        var jsonDiv = $('<div class="jsonObjectView"></div>');
        $.each(jsonObj, function (key, value) {
            if (value) {
                // key is either an array index or object key                    
                var children = GetHTMLForDefaultView(value);

                if (typeof children == "object") {
                    $(jsonDiv).append($('<span class="arrayNameView"/>').text(key));
                    $(jsonDiv).append(children);
                } else {
                    var childDiv = $('<div class="jsonObjectMemberView"></div>');
                    $(childDiv).append($('<span class="memberNameView"/>').text(key));
                    $(childDiv).append($('<span class="memberValueView"/>').text(children));
                    $(jsonDiv).append(childDiv);
                }
            } else {
                var childDiv = $('<div class="jsonObjectMemberView"></div>');
                $(childDiv).append($('<span class="memberNameView"/>').text(key));
                $(childDiv).append('<span class="memberValueView"></span>');
                $(jsonDiv).append(childDiv);
            }
        });
        return jsonDiv;
    }
    else {
        // jsonOb is a number or string
        return jsonObj;
    }
}
