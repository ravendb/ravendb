var numPages;
        var pageNumber = 1;
        var pageSize = 25;
        var allDocsTotalCount;
        var indexName;
        var indexValues;
        var isInQueryMode = false;

        $(document).ready(function () {
            DivanUI.GetDocumentCount(function (count) {
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

            $('#txtIndexName').autocomplete({
                source: function (request, response) {
                    DivanUI.SearchIndexes(request.term, function (searchResult) {
                        response(searchResult);
                    });
                },
                minLength: 2,
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
            $('#docList').show().html('<img src="images/ajax-loader.gif" /> Loading...');
            DivanUI.GetDocumentPage(pageNumber, pageSize, function (docs) {
                if (docs.length == 0) {
                    $('#docList').html('There are no documents in your database.');
                    $('#pager').hide();
                } else {
                    processDocumentResults(docs, allDocsTotalCount);
                }
            });
        }

        function ExecuteQuery() {
            isInQueryMode = true;

            $('#docList').show().html('<img src="images/ajax-loader.gif" /> Loading...');

            DivanUI.QueryIndex(indexName, indexValues, pageNumber, pageSize, function (data) {
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

        function getDisplayString(json) {
            var returnJSON = json;
            delete returnJSON["@metadata"];
            var jsonString = JSON.stringify(returnJSON);
            if (jsonString.length > 90)
                jsonString = jsonString.substring(0,90) + '...';
            return jsonString;
        }

        function processDocumentResults(results, totalCount) {
            numPages = Math.ceil(totalCount / pageSize);
            $("#pager").pager({
                pagenumber: pageNumber,
                pagecount: numPages,
                buttonClickCallback: pagerClick
            });

            if (!$('#docList').hasTemplate()) {
                $('#docList').setTemplateURL('JSONTemplates/documentPage.html');
            }            

            $('#docList').processTemplate(results);
            $('.searchListItem').each(function () {
                $(this).click(function () {
                    EditDocument($(this).attr('id'));
                });

                var previousBGColor;
                $(this).hover(function () {
                    previousBGColor =  $(this).css('background-color');
                    $(this).css('background-color', '#94C2D8');
                    var x = $(this).position().left + $(this).outerWidth();
                    var y = $('.searchListWrapper').position().top;
                    var width = $('#body').width() - x - 20;
                    var unParsedJSON = unescape($(this).children('.searchListItemValue').html());
                    var json = JSON.parse(unParsedJSON);
                    if (json["@metadata"])
                        delete json["@metadata"];
                    var jsonPreview = $('<div class="jsonViewWrapper"></div>');                    
                    $(jsonPreview).html(JSONToViewHTML(json));
                    $(jsonPreview).find('.jsonObjectView:first').css('border', 'none').css('padding', '0').css('margin-left', '0');
                    $(jsonPreview).prepend("<h2>Click to edit this document</h2><h3>Document Preview:</h3>");  
                    $('#documentPreview')
                        .css('width', width +'px')
                        .css('position', 'absolute')
                        .css('left', x + 'px')
                        .css('top', y + 'px')                        
                        .html(jsonPreview);
                    if ($('#documentPreview').is(":animated"))
                            $('#documentPreview').stop().show().fadeTo("normal", 0.9);
                    else
                        $('#documentPreview').is(':visible') ? $('#documentPreview').fadeTo("normal", 0.9) : $('#documentPreview').fadeIn();                        
                }, function () {
                    $(this).css('background-color', previousBGColor);
                    if ($('#documentPreview').is(':animated'))
                        $('#documentPreview').stop().fadeTo("normal", 0, function() { $(this).hide() });
                    else
                        $('#documentPreview').stop().fadeOut();
                });
            });
        }

        function pagerClick(newPageNumber) {
            pageNumber = newPageNumber;
            if (!isInQueryMode) {
                getAllDocuments();
            } else {
                ExecuteQuery();
            }
        }

        function EditDocument(id) {
            $('#ajaxSuccess, #ajaxError').fadeOut();            
            DivanUI.GetDocument(id, function (doc, etag) { 
                ShowEditorForDocument(id, doc, etag, 'Edit Document', function(id, etag, json, editor) {
                    DivanUI.SaveDocument(id, etag, GetJSONFromEditor(), function () {
                        $(editor).dialog('close');
                        $('#ajaxSuccess').html('Your document has been updated. Click <a href="#" onclick="EditDocument(\'' + id + '\'); return false;">here</a> to see it again.').fadeIn('slow');
                        if (!isInQueryMode) {
                            getAllDocuments();
                        } else {
                            ExecuteQuery();
                        }
                    });
                }, function(id, etag, editor, deleteDialog) {
                    DivanUI.DeleteDocument(id, etag, function(data) {
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
            ShowEditorForNewDocument(function(json, editor) {
                DivanUI.SaveDocument(null, null, json, function (data) {
                    $(editor).dialog('close');
                    $('#ajaxSuccess').html('Your document has been created. Click <a href="#" onclick="EditDocument(\'' + data.id + '\'); return false;">here</a> to see it again.').fadeIn('slow');
                    if (!isInQueryMode) {
                        getAllDocuments();
                    } else {
                        ExecuteQuery();
                    }
                });
            });            
        }

        function SaveDocument(id, json) {
            DivanUI.SaveDocument(id, JSON.stringify(json), function () { });
        }
